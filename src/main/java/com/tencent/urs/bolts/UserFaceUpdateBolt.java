package com.tencent.urs.bolts;

import java.lang.ref.SoftReference;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import NewsApp.Newsapp.NewsAttr;
import NewsApp.Newsapp.NewsCategory;
import NewsApp.Newsapp.NewsIndex;
import NewsApp.Newsapp.UserFace;
import NewsProcessor.NewsClassifier;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.tencent.monitor.MonitorTools;
import com.tencent.streaming.commons.bolts.config.AbstractConfigUpdateBolt;
import com.tencent.streaming.commons.spouts.tdbank.Output;
import com.tencent.tde.client.Result;
import com.tencent.tde.client.TairClient.TairOption;

import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBackContext;

import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.Utils;

public class UserFaceUpdateBolt extends AbstractConfigUpdateBolt{
	private static final long serialVersionUID = 2972911860800045348L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private DataCache<NewsApp.Newsapp.NewsAttr> itemAttrCache;
	//private DataCache<UserFace> qqProfileCache;

	private HashMap<String, HashSet<String>> liveCombinerMap;
	private int nsCbIndexTableId;
	private int nsUserFaceTableId;
	private int nsConfigTableId;
	private int dataExpireTime;
	//private int cacheExpireTime;
	private int topNum;
	private boolean debug;
	private UpdateCallBack userFaceCallBack;
	
	public class ConfigValue{
		String value;
		long lastUpdateTime;
		
		ConfigValue(String value,long lastUpdateTime){
			this.value = value;
			this.lastUpdateTime = lastUpdateTime;
		}
		
		public String getValue(){
			return this.value;
		}
		
		public long getTime(){
			return this.lastUpdateTime;
		}
	}
	
	private HashMap<String,ConfigValue> blackPreferenceMap;
	private HashMap<String,ConfigValue> actionWeightMap;	
		
	public UserFaceUpdateBolt(String config, ImmutableList<Output> outputField) {
		super(config, outputField, Constants.config_stream);
	}

	@Override
	public void updateConfig(XMLConfiguration config) {
		nsCbIndexTableId = config.getInt("item_index_table",519);
		nsUserFaceTableId = config.getInt("user_face_table",54);
		nsConfigTableId = config.getInt("config_table",55);
		dataExpireTime = config.getInt("data_expiretime",30*24*3600);
		//cacheExpireTime = config.getInt("cache_expiretime",3600);
		debug = config.getBoolean("debug",false);
		topNum = config.getInt("top_num",100);
	}
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		super.prepare(conf, context, collector);
		updateConfig(super.config);
	
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.itemAttrCache = new DataCache<NewsApp.Newsapp.NewsAttr>(conf);
		//this.qqProfileCache = new DataCache<UserFace>(conf);
		
		this.liveCombinerMap = new HashMap<String,HashSet<String>>(1024);
		this.userFaceCallBack = new UpdateCallBack(mt, this.nsCbIndexTableId,debug);		
		this.blackPreferenceMap = new HashMap<String,ConfigValue>();
		this.actionWeightMap = new HashMap<String,ConfigValue>();
		
		blackPreferenceInit();
		actionWeightMapInit();
		int combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5);
		setCombinerTime(combinerExpireTime);
	}
	
	private static Logger logger = LoggerFactory.getLogger(UserFaceUpdateBolt.class);
	
	private String getConfigValueFromTde(String key){
		ClientAttr clientEntry = mtClientList.get(0);		
		TairOption opt = new TairOption(clientEntry.getTimeout());
		try {
			Result<byte[]> res = clientEntry.getClient().get((short)nsConfigTableId, key.getBytes(), opt);
			if(res.isSuccess() && res.getResult() != null){
				logger.info("get config from tde, value="+new String(res.getResult(),"UTF-8"));
				return new String(res.getResult(),"UTF-8");
			}
		} catch(Exception e){
		}
		
		return null;
	}
	
	private float computerUserFaceWeight(float level, float time_loss, String actionType){
		String typeWeight = null;
		
		long now = System.currentTimeMillis()/1000;
		if(actionWeightMap.containsKey(actionType)){
			long lastUpdateTime = actionWeightMap.get(actionType).getTime();
			if(lastUpdateTime > now - 3600){
				typeWeight = actionWeightMap.get(actionType).getValue();
			}else{
				typeWeight = getConfigValueFromTde(actionType);
				ConfigValue confValue = new ConfigValue(typeWeight,now);
				actionWeightMap.put(actionType, confValue);
			}
		}else{
			typeWeight = getConfigValueFromTde(actionType);
			ConfigValue confValue = new ConfigValue(typeWeight,now);
			actionWeightMap.put(actionType, confValue);
		}
		
		if(typeWeight != null){
			return level*time_loss*Float.valueOf(typeWeight);
		}else{
			return 0F;
		}
		
	}
	
	private void blackPreferenceInit(){
		String black_preference = "新闻|要闻|订阅|图片|视频|精选|北京|上海|广州|深圳|重庆|天津|香港|澳门|山东|山西|河南|河北|湖南|湖北|广东|广西|黑龙江|辽宁|浙江|安徽|江苏|福建|甘肃|江西|云南|贵州|四川|青海|陕西|吉林|宁夏|海南|西藏|内蒙古|新疆|台湾";
		String[] bpres =  black_preference.split("\\|",-1);
		
		long now = System.currentTimeMillis()/1000;
		for(String each:bpres){
			ConfigValue confValue = new ConfigValue(each,now);
			this.blackPreferenceMap.put(each, confValue);
		}
	}
	
	private void actionWeightMapInit(){
		long now = System.currentTimeMillis()/1000;
		this.actionWeightMap.put("1", new ConfigValue("0",now));
		this.actionWeightMap.put("5", new ConfigValue("1.5",now));
		this.actionWeightMap.put("9", new ConfigValue("0.4",now));
		this.actionWeightMap.put("10", new ConfigValue("0.4",now));
		this.actionWeightMap.put("11", new ConfigValue("0.4",now));
		this.actionWeightMap.put("13", new ConfigValue("0.5",now));
		this.actionWeightMap.put("16", new ConfigValue("0.3",now));
		this.actionWeightMap.put("17", new ConfigValue("0.3",now));
		this.actionWeightMap.put("18", new ConfigValue("0.3",now));
		this.actionWeightMap.put("19", new ConfigValue("2",now));
		this.actionWeightMap.put("20", new ConfigValue("2",now));
		
		this.actionWeightMap.put("21", new ConfigValue("0.3",now));
		this.actionWeightMap.put("22", new ConfigValue("0",now));
		this.actionWeightMap.put("23", new ConfigValue("0",now));
		this.actionWeightMap.put("24", new ConfigValue("0",now));
		this.actionWeightMap.put("25", new ConfigValue("0.3",now));
		this.actionWeightMap.put("26", new ConfigValue("0",now));
	}
	
	private boolean isBlackPreference(String prefernece){	
		String newResult = null;
		long now = System.currentTimeMillis()/1000;
		if(blackPreferenceMap.containsKey(prefernece)){
			long lastUpdateTime = blackPreferenceMap.get(prefernece).getTime();
			if(lastUpdateTime > now - 3600){
				return true;
			}else{
				newResult = getConfigValueFromTde("black_preference_list");
			}
		}else{
			newResult = getConfigValueFromTde("black_preference_list");

		}
		
		if(newResult != null){
			String[] items = newResult.split("\\|",-1);
			for(String each:items){
				ConfigValue confValue = new ConfigValue(each,now);
				actionWeightMap.put(each, confValue);
			}
		}
		
		if(blackPreferenceMap.containsKey(prefernece)){
			return true;
		}else{
			return false;
		}
	}
	
	private void setCombinerTime(final int second) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					while (true) {
						Thread.sleep(second * 1000);
						HashMap<String,HashSet<String>> deadCombinerMap = null;
						synchronized (liveCombinerMap) {
							deadCombinerMap = liveCombinerMap;
							liveCombinerMap = new HashMap<String,HashSet<String>>(1024);
						}
						
						Set<String> keySet = deadCombinerMap.keySet();
						for (String key : keySet) {
							HashSet<String> valueSet = deadCombinerMap.get(key);
							try{								
								new GetItemAttrByItemId(key,valueSet).excute();
							}catch(Exception e){
								logger.error(e.getMessage(), e);
							}
						}
						deadCombinerMap.clear();
						deadCombinerMap = null;
					}
				} catch (Exception e) {
					logger.error("Schedule thread error:" + e, e);
				}
			}
		}).start();
	}
	
	private void combinerKeys(String key,String  value) {	
		synchronized(liveCombinerMap){
			HashSet<String> valueSet = null;
			if(liveCombinerMap.containsKey(key)){
				valueSet = liveCombinerMap.get(key);	
			}else{
				valueSet = new HashSet<String>();
			}
			valueSet.add(value);
			liveCombinerMap.put(key, valueSet);
		}
	}	
	
	@Override
	public void processEvent(String sid, Tuple tuple) {	
		try{
			
			String bid = tuple.getStringByField("bid");
			String topic = tuple.getStringByField("topic");
			String itemId = tuple.getStringByField("item_id");
			
			if(topic.equals(Constants.actions_stream)){
				String qq = tuple.getStringByField("qq");
				String actionType = tuple.getStringByField("action_type");
				String comValue = Utils.spliceStringBySymbol("#", bid, qq, actionType);
				
				String comKey = Utils.spliceStringBySymbol("#", bid,itemId);
				combinerKeys(comKey, comValue);
			}
		}catch(Exception e){
			logger.error(e.getMessage(), e);
		}

	}

	public class GetItemAttrByItemId implements MutiClientCallBack{
		private String bidItemId;
		private HashSet<String> valueSet;
		
		public GetItemAttrByItemId(String bidItemId,HashSet<String> valueSet){
			this.bidItemId = bidItemId;
			this.valueSet = valueSet;
		}
		
		@Override
		public void handle(Future<?> future, Object context) {
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() != null){
					NewsApp.Newsapp.NewsAttr itemAttr = NewsApp.Newsapp.NewsAttr.parseFrom(res.getResult());
					next(valueSet,itemAttr);
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}

		public void excute() {
			NewsApp.Newsapp.NewsAttr itemAttr = null;
			SoftReference<NewsApp.Newsapp.NewsAttr> sr = itemAttrCache.get(bidItemId);
			if(sr != null){
				itemAttr = sr.get();
			}
				
			if(itemAttr != null){
				next(valueSet,itemAttr);
			}else{				
				try{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsCbIndexTableId,bidItemId.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}catch(Exception e){
					logger.error(e.getMessage(), e);
				}
			}
		}
		
		public void next(HashSet<String> valueSet,NewsApp.Newsapp.NewsAttr itemAttr){	
			for(String bidQQType: valueSet){
				if(bidQQType.indexOf("389687043") > 0){
					logger.info("user face,qq=389687043,itemAttr.count ="+itemAttr.getCategoryCount());
				}
				new RefreshQQProfileCallBack(bidQQType,itemAttr).excute();
			}
		}
		
	} 
	
	public class RefreshQQProfileCallBack implements MutiClientCallBack{
		private String bidQQType;
		private String key;
		private String actionType;
		NewsApp.Newsapp.NewsAttr itemAttr;
		
		public RefreshQQProfileCallBack(String bidQQType,NewsApp.Newsapp.NewsAttr itemAttr){
			this.bidQQType = bidQQType;
			
			String[] items =  bidQQType.split("#");
			if(items.length >= 2){
				this.key = Utils.spliceStringBySymbol("#", items[0],items[1]);
				this.actionType = items[2];
			}else{
				this.key = null;
				this.actionType = null;
			}
			
			this.itemAttr = itemAttr;
		}
		
		@Override
		public void handle(Future<?> future, Object context) {
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			if(key.indexOf("389687043")>=0){
				logger.info("user face,from tde=");
			}
			
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() != null){
					UserFace qqFace = UserFace.parseFrom(res.getResult());					
					doRefreshUserFace(qqFace);
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}

		public void excute() {
			if(key == null || actionType == null){
				return;
			}
				
			try{
				ClientAttr clientEntry = mtClientList.get(0);		
				TairOption opt = new TairOption(clientEntry.getTimeout());
				Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsUserFaceTableId,key.getBytes(),opt);
				clientEntry.getClient().notifyFuture(future, this,clientEntry);	
			}catch(Exception e){
				logger.error(e.getMessage(), e);
			}
			
			if(key.indexOf("389687043")>=0){
				logger.info("user face,send to tde,tableId="+nsUserFaceTableId+",key="+key);
			}
		}
		
		private void doRefreshUserFace(UserFace qqFace){
			if(key.indexOf("389687043")>=0){
				logger.info("user face,qqFace="+qqFace.getPreferenceCount());
			}
			LinkedList<UserFace.UserPreference> newQQFaceList = new LinkedList<UserFace.UserPreference>();
	
			HashSet<String> alreadyIn = new HashSet<String>();
			for(NewsCategory cate:itemAttr.getCategoryList()){
				if(cate.getName().toStringUtf8().equals("")){
					continue;
				}
				
				for(UserFace.UserPreference up:qqFace.getPreferenceList()){
					String tag = up.getPreference().toStringUtf8();
					
					if(newQQFaceList.size() > topNum){
						break;
					}
					
					if(tag.equals("") || isBlackPreference(tag)){
						continue;
					}
														
					if(tag.equals(cate.getName().toStringUtf8())){
						UserFace.UserPreference.Builder newUpBuilder = 
								UserFace.UserPreference.newBuilder();
						
						float newWeight = up.getWeight() + computerUserFaceWeight(1F ,0.25F ,actionType);
						newUpBuilder.setPreference(cate.getName());
						newUpBuilder.setLevel(cate.getLevel());
						newUpBuilder.setType(up.getType());
						newUpBuilder.setWeight(newWeight);
						
						newQQFaceList.add(newUpBuilder.build());
						alreadyIn.add(cate.getName().toStringUtf8());
					}
				}
				
				if(!alreadyIn.contains(cate.getName().toStringUtf8())){
					UserFace.UserPreference.Builder newUpBuilder = 
							UserFace.UserPreference.newBuilder();
					
					float newWeight = computerUserFaceWeight(1F ,0.25F ,actionType);
					
					newUpBuilder.setPreference(cate.getName());
					newUpBuilder.setLevel(cate.getLevel());
					newUpBuilder.setType(1);
					newUpBuilder.setWeight(newWeight);
					newQQFaceList.add(newUpBuilder.build());
					
					alreadyIn.add(cate.getName().toStringUtf8());
				}
			}
			
			for(UserFace.UserPreference up:qqFace.getPreferenceList()){
				if(!alreadyIn.contains(up.getPreference().toStringUtf8())){
					newQQFaceList.add(up);
					alreadyIn.add(up.getPreference().toStringUtf8());
				}
			}
					
			Collections.sort(newQQFaceList, new Comparator<UserFace.UserPreference>() {   
				@Override
				public int compare(UserFace.UserPreference arg0,
						UserFace.UserPreference arg1) {
					if(arg0.getWeight() > arg1.getWeight()){
						return -1;
					}
					return 1;
				}
			}); 
			
			UserFace.Builder newQQFaceBuilder = UserFace.newBuilder();
			newQQFaceBuilder.setProfile(qqFace.getProfile());
			newQQFaceBuilder.addAllPreference(newQQFaceList);
			
			saveInTde(key,newQQFaceBuilder.build());
		}
		
		private void saveInTde(String key,UserFace value){
			if(key.indexOf("389687043")>=0){
				logger.info("user face,count="+value.getPreferenceCount());
			}
			
			if(key.indexOf("475182144")>=0){
				logger.info("user face,count="+value.getPreferenceCount());
			}
			
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
				try {
					Future<Result<Void>> future = 
					clientEntry.getClient().putAsync((short)nsUserFaceTableId, 
										this.key.getBytes(), value.toByteArray(), putopt);
					clientEntry.getClient().notifyFuture(future, userFaceCallBack, 
							new UpdateCallBackContext(clientEntry,key,value.toByteArray(),putopt));

				} catch (Exception e){
					logger.error(e.getMessage(), e);
				}
			}
		}
	}
}