package com.tencent.urs.bolts;

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

import NewsApp.Newsapp.NewsCategory;
import NewsApp.Newsapp.UserFace;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.google.common.collect.ImmutableList;
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
import com.tencent.urs.utils.LRUCache;
import com.tencent.urs.utils.Utils;

public class UserFaceUpdateBolt extends AbstractConfigUpdateBolt{
	private static final long serialVersionUID = 2972911860800045348L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private LRUCache<String, NewsApp.Newsapp.NewsAttr> itemAttrCache;
	
	private HashMap<String, String> liveCombinerMap;
	private int nsCbIndexTableId;
	private int nsUserFaceTableId;
	private int nsConfigTableId;
	private int dataExpireTime;
	private int topNum;
	private boolean debug;
	private UpdateCallBack userFaceCallBack;
	
	private long pbLastUpdateTime;
	
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
	
	private HashSet<String> blackPreferenceMap;
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
		debug = config.getBoolean("debug",false);
		topNum = config.getInt("top_num",10);
	}
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		super.prepare(conf, context, collector);
		updateConfig(super.config);
	
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.itemAttrCache = new LRUCache<String, NewsApp.Newsapp.NewsAttr>(5000);
		
		this.liveCombinerMap = new HashMap<String,String>(1024);
		this.userFaceCallBack = new UpdateCallBack(mt, this.nsCbIndexTableId,debug);		
		this.blackPreferenceMap = new HashSet<String>();
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
			}else{
				logger.info("get config from tde failed! not found key="+key);
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
				if(typeWeight != null){
					ConfigValue confValue = new ConfigValue(typeWeight,now);
					actionWeightMap.put(actionType, confValue);
				}else{
					typeWeight = actionWeightMap.get(actionType).getValue();
					ConfigValue confValue = new ConfigValue(typeWeight,now);
					actionWeightMap.put(actionType, confValue);
				}
				logger.info("get actionType from tde , type="+actionType+",value="+typeWeight);
			}
		}else{
			typeWeight = getConfigValueFromTde(actionType);
			if(typeWeight != null){
				ConfigValue confValue = new ConfigValue(typeWeight,now);
				actionWeightMap.put(actionType, confValue);
			}else{
				typeWeight = actionWeightMap.get(actionType).getValue();
				ConfigValue confValue = new ConfigValue(typeWeight,now);
				actionWeightMap.put(actionType, confValue);
			}
			logger.info("get actionType from tde failed, type="+actionType);
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
		
		this.pbLastUpdateTime = System.currentTimeMillis()/1000;
		for(String each:bpres){
			this.blackPreferenceMap.add(each);
		}
		logger.info("blackPreferenceInit,size= "+blackPreferenceMap.size());
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
		
		
		logger.info("actionWeightMap,size= "+actionWeightMap.size());
	}
	
	private boolean isBlackPreference(String prefernece){	
		
		long now = System.currentTimeMillis()/1000;
		if(pbLastUpdateTime < now - 3600){
			String newResult = getConfigValueFromTde("black_preference_list");
			if(newResult != null){
				String[] items = newResult.split("\\|",-1);
				if(items.length > 0){
					blackPreferenceMap.clear();
					for(String each:items){
						blackPreferenceMap.add(each);
					}
				}
			}
			pbLastUpdateTime = now;
		}
		
		if(blackPreferenceMap.contains(prefernece)){
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
						HashMap<String,String> deadCombinerMap = null;
						synchronized (liveCombinerMap) {
							deadCombinerMap = liveCombinerMap;
							liveCombinerMap = new HashMap<String,String>(1024);
						}
						
						Set<String> keySet = deadCombinerMap.keySet();
						for (String key : keySet) {
							String itemActType = deadCombinerMap.get(key);
							try{								
								new GetItemAttrByItemId(key,itemActType).excute();
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
			liveCombinerMap.put(key, value);
		}
	}	
	
	@Override
	public void processEvent(String sid, Tuple tuple) {	
		try{
			
			String bid = tuple.getStringByField("bid");
			String itemId = tuple.getStringByField("item_id");
			String qq = tuple.getStringByField("qq");
			String uid = tuple.getStringByField("uid");
			String actionType = tuple.getStringByField("action_type");
			
			if(!Utils.isBidValid(bid) || !Utils.isItemIdValid(itemId)
					|| !Utils.isQNumValid(qq)){
				return;
			}

			String comValue = Utils.spliceStringBySymbol("#", bid, itemId, actionType);
			String comKey = Utils.spliceStringBySymbol("#", bid,qq);
			combinerKeys(comKey, comValue);
		}catch(Exception e){
			logger.error(e.getMessage(), e);
		}

	}

	public class GetItemAttrByItemId implements MutiClientCallBack{
		private String bidQQ;
		private String bidItem;
		private String actType;
		
		public GetItemAttrByItemId(String bidQQ,String bidItemActType){
			this.bidQQ = bidQQ;
			
			String[] items = bidItemActType.split("#",-1);
			if(items.length >=3){
				this.bidItem = Utils.spliceStringBySymbol("#", items[0], items[1]);
				this.actType = items[2];
			}
		}
		
		@Override
		public void handle(Future<?> future, Object context) {
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() != null){
					NewsApp.Newsapp.NewsAttr itemAttr = NewsApp.Newsapp.NewsAttr.parseFrom(res.getResult());
					itemAttrCache.put(bidItem, itemAttr);
					next(bidQQ,itemAttr,actType);
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}

		public void excute() {
			if(bidItem == null || actType == null){
				return;
			}
			
			if(itemAttrCache.containsKey(bidItem)){
				NewsApp.Newsapp.NewsAttr itemAttr = itemAttrCache.get(bidItem);
				next(bidQQ,itemAttr,actType);
			}else{				
				try{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsCbIndexTableId,bidItem.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}catch(Exception e){
					logger.error(e.getMessage(), e);
				}
			}
		}
		
		public void next(String bidQQ,NewsApp.Newsapp.NewsAttr itemAttr,String actType){	
			if(bidQQ.indexOf("389687043") > 0){
				logger.info("user face,qq=389687043,itemAttr.count ="+itemAttr.getCategoryCount());
			}
			
			if(itemAttr == null || itemAttr.getCategoryCount() <= 0){
				return;
			}
			new RefreshQQProfileCallBack(bidQQ,itemAttr,actType).excute();
		}
	} 
	
	public class RefreshQQProfileCallBack implements MutiClientCallBack{
		private String bidQQ;
		private String actionType;
		NewsApp.Newsapp.NewsAttr itemAttr;
		
		public RefreshQQProfileCallBack(String bidQQ,NewsApp.Newsapp.NewsAttr itemAttr,String actionType){			
			this.bidQQ = bidQQ;
			this.actionType = actionType;
			this.itemAttr = itemAttr;
		}
		
		@Override
		public void handle(Future<?> future, Object context) {
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			if(bidQQ.indexOf("389687043")>=0){
				logger.info("user face,from tde=");
			}
			UserFace qqFace = null;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() != null){
					qqFace = UserFace.parseFrom(res.getResult());					
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
			
			doRefreshUserFace(qqFace);
		}

		public void excute() {
			try{
				ClientAttr clientEntry = mtClientList.get(0);		
				TairOption opt = new TairOption(clientEntry.getTimeout());
				Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsUserFaceTableId,bidQQ.getBytes(),opt);
				clientEntry.getClient().notifyFuture(future, this,clientEntry);	
			}catch(Exception e){
				logger.error(e.getMessage(), e);
			}
			
			if(bidQQ.indexOf("389687043")>=0){
				logger.info("user face,send to tde,tableId="+nsUserFaceTableId+",bidQQ="+bidQQ);
			}
		}
		
		private void insertToListByWeightDesc(UserFace.UserPreference newBaseVal,
				LinkedList<UserFace.UserPreference> qqFaceList) {
			boolean insert = false;
			for (int idx = 0; idx < qqFaceList.size(); idx++) {
				UserFace.UserPreference oldVal = qqFaceList.get(idx);
				if (!insert && newBaseVal.getWeight() >= oldVal.getWeight()) {
					qqFaceList.add(idx, newBaseVal);
					idx++;
					insert = true;
				}
				
				if (oldVal.getPreference().equals(newBaseVal.getPreference())) {
					qqFaceList.remove(oldVal);
					idx--;
				}
			}
			
			if (!insert && qqFaceList.size() < topNum) {
				qqFaceList.add(newBaseVal);
			}
		}

		
		private void doRefreshUserFace(UserFace qqFace){
			if(bidQQ.indexOf("389687043")>=0){
				logger.info("user face,oldQQFace="+qqFace.getPreferenceCount()+",newItemAttrFace="+itemAttr.getCategoryCount());
			}
			LinkedList<UserFace.UserPreference> newQQFaceList = new LinkedList<UserFace.UserPreference>();
	
			HashSet<String> alreadyIn = new HashSet<String>();
			
			for(NewsCategory cate:itemAttr.getCategoryList()){	
				if(cate.getName().toStringUtf8().equals("")){
					continue;
				}
				
				if(qqFace != null){
					for(UserFace.UserPreference up:qqFace.getPreferenceList()){
						String tag = up.getPreference().toStringUtf8();
						
						if(newQQFaceList.size() > topNum){
							break;
						}
						
						if(tag.equals("") || isBlackPreference(tag)){
							continue;
						}
															
						if(tag.equals(cate.getName().toStringUtf8())
								&& !alreadyIn.contains(tag)){
							UserFace.UserPreference.Builder newUpBuilder = 
									UserFace.UserPreference.newBuilder();
							
							float newWeight = up.getWeight() + computerUserFaceWeight(1F ,0.25F ,actionType);
							if(bidQQ.indexOf("389687043")>=0){
								logger.info("add new pre in for,newWeight = "+newWeight+",type="+actionType+",weightInMap="+actionWeightMap.get(actionType));
							}
							if(newWeight > 0){
								newUpBuilder.setPreference(cate.getName());
								newUpBuilder.setLevel(cate.getLevel());
								newUpBuilder.setType(up.getType());
								newUpBuilder.setWeight(newWeight);
								
								//newQQFaceList.add(newUpBuilder.build());
								insertToListByWeightDesc(newUpBuilder.build(),newQQFaceList);
								alreadyIn.add(cate.getName().toStringUtf8());
							}
						}
					}
				}
				
				if(!alreadyIn.contains(cate.getName().toStringUtf8())){
					UserFace.UserPreference.Builder newUpBuilder = 
							UserFace.UserPreference.newBuilder();
					
					float newWeight = computerUserFaceWeight(1F ,0.25F ,actionType);
					if(bidQQ.indexOf("389687043")>=0){
						logger.info("add new pre in outside,newWeight = "+newWeight+",type="+actionType+",weightInMap="+actionWeightMap.get(actionType));
					}
					if(newWeight > 0){
						newUpBuilder.setPreference(cate.getName());
						newUpBuilder.setLevel(cate.getLevel());
						newUpBuilder.setType(1);
						newUpBuilder.setWeight(newWeight);
						//newQQFaceList.add(newUpBuilder.build());
						insertToListByWeightDesc(newUpBuilder.build(),newQQFaceList);
						alreadyIn.add(cate.getName().toStringUtf8());
					}
				}
			}
			
			if(bidQQ.indexOf("389687043")>=0){
				logger.info("1,user face,newQQFaceList.size="+newQQFaceList.size());
			}
			
			for(UserFace.UserPreference up:qqFace.getPreferenceList()){
				if(newQQFaceList.size() > topNum){
					break;
				}
				
				if(!alreadyIn.contains(up.getPreference().toStringUtf8()) 
						&& up.getWeight() > 0){
					insertToListByWeightDesc(up,newQQFaceList);
					alreadyIn.add(up.getPreference().toStringUtf8());
				}
			}
			if(bidQQ.indexOf("389687043")>=0){
				logger.info("2,user face,newQQFaceList.size="+newQQFaceList.size());
			}
			
			/*Collections.sort(newQQFaceList, new Comparator<UserFace.UserPreference>() {   
				@Override
				public int compare(UserFace.UserPreference arg0,UserFace.UserPreference arg1) {
					if(arg0.getWeight() > arg1.getWeight()){
						return -1;
					}
					return 1;
				}
			});*/ 
			
			UserFace.Builder newQQFaceBuilder = UserFace.newBuilder();
			newQQFaceBuilder.setProfile(qqFace.getProfile());
			newQQFaceBuilder.addAllPreference(newQQFaceList);
			
			saveInTde(bidQQ,newQQFaceBuilder.build());
		}
		
		private void saveInTde(String key,UserFace value){
			if(key.indexOf("389687043")>=0 || key.indexOf("475182144")>=0){
				logger.info("user face,count="+value.getPreferenceCount());
			}
			
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
				try {
					Future<Result<Void>> future = 
					clientEntry.getClient().putAsync((short)nsUserFaceTableId, 
										key.getBytes(), value.toByteArray(), putopt);
					clientEntry.getClient().notifyFuture(future, userFaceCallBack, 
							new UpdateCallBackContext(clientEntry,key,value.toByteArray(),putopt));

				} catch (Exception e){
					logger.error(e.getMessage(), e);
				}
			}
		}
	}
}