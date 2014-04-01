package com.tencent.urs.bolts;

import java.io.UnsupportedEncodingException;
import java.lang.ref.SoftReference;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import NewsApp.Newsapp.NewsAttr;
import NewsApp.Newsapp.NewsCategory;
import NewsApp.Newsapp.NewsIndex;
import NewsApp.Newsapp.UserFace;
import NewsApp.Newsapp.UserFace.UserPreference;
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

public class CBBolt extends AbstractConfigUpdateBolt{
	private static final long serialVersionUID = 2972911860800045348L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private DataCache<NewsApp.Newsapp.NewsAttr> itemAttrCache;
	private DataCache<UserFace> qqProfileCache;

	private ConcurrentHashMap<String, HashSet<String>> combinerMap;
	private NewsClassifier classifier;
	private int nsCbIndexTableId;
	private int nsUserFaceTableId;
	private int dataExpireTime;
	private int cacheExpireTime;
	private int topNum;
	private boolean debug;
	private UpdateCallBack putCallBack;
		
	public CBBolt(String config, ImmutableList<Output> outputField) {
		super(config, outputField, Constants.config_stream);
	}

	@Override
	public void updateConfig(XMLConfiguration config) {
		nsCbIndexTableId = config.getInt("item_index_table",519);
		nsUserFaceTableId = config.getInt("user_face_table",53);
		dataExpireTime = config.getInt("data_expiretime",30*24*3600);
		cacheExpireTime = config.getInt("cache_expiretime",3600);
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
		this.qqProfileCache = new DataCache<UserFace>(conf);
		
		this.combinerMap = new ConcurrentHashMap<String,HashSet<String>>(1024);			
		this.putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_send_interfaceID, this.getClass().getName());
		
		this.classifier = new NewsClassifier();
		int combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5);
		setCombinerTime(combinerExpireTime);
	}
	
	private static Logger logger = LoggerFactory.getLogger(CBBolt.class);
	
	private void setCombinerTime(final int second) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					while (true) {
						Thread.sleep(second * 1000);
						Set<String> keySet = combinerMap.keySet();
						for (String key : keySet) {
							HashSet<String> valueSet = combinerMap.remove(key);
							try{								
								new GetItemAttrByItemId(key,valueSet).excute();
							}catch(Exception e){
								//mt.addCountEntry(systemID, interfaceID, item, count)
							}
						}
					}
				} catch (Exception e) {
					logger.error("Schedule thread error:" + e, e);
				}
			}
		}).start();
	}
	
	private void combinerKeys(String key,String  value) {	
		synchronized(combinerMap){
			HashSet<String> valueSet = null;
			if(combinerMap.containsKey(key)){
				valueSet = combinerMap.get(key);	
			}else{
				valueSet = new HashSet<String>();
			}
			valueSet.add(value);
			combinerMap.put(key, valueSet);
		}
	}	

	private static void genInputPbBuilder(NewsAttr.Builder cbAppBuilder,
					String itemId,Long bigType,Long midType,Long smallType,
					com.google.protobuf.ByteString bigTypeName,
					com.google.protobuf.ByteString midTypeName, 
					com.google.protobuf.ByteString smallTypeName,
					com.google.protobuf.ByteString text){
		cbAppBuilder.setNewsId(itemId);
		
		NewsCategory.Builder bigTypeBuilder = NewsCategory.newBuilder();
		NewsCategory.Builder midTypeBuilder = NewsCategory.newBuilder();
		NewsCategory.Builder smallTypeBuilder = NewsCategory.newBuilder();
		
		bigTypeBuilder.setId(bigType);
		midTypeBuilder.setId(midType);
		smallTypeBuilder.setId(smallType);
		
		bigTypeBuilder.setLevel(0);
		midTypeBuilder.setLevel(1);
		smallTypeBuilder.setLevel(2);

		
		bigTypeBuilder.setName(bigTypeName);
		midTypeBuilder.setName(midTypeName);
		smallTypeBuilder.setName(smallTypeName);
		
		cbAppBuilder.addCategory(0,bigTypeBuilder.build());
		cbAppBuilder.addCategory(1,midTypeBuilder.build());
		cbAppBuilder.addCategory(2,smallTypeBuilder.build());
		
	
		cbAppBuilder.setTitle(text);
		cbAppBuilder.setSourceId(Long.valueOf(bigType));

	}
	
	@Override
	public void processEvent(String sid, Tuple tuple) {	
		try{
			
			String bid = tuple.getStringByField("bid");
			String topic = tuple.getStringByField("topic");
			String itemId = tuple.getStringByField("item_id");
			
			if(topic.equals(Constants.item_info_stream) ){
				String checkKey = bid+"#"+itemId;
				if(itemAttrCache.hasKey(checkKey)){
					return;
				}
				
				String bigType = tuple.getStringByField("cate_id1");
				String midType = tuple.getStringByField("cate_id2");
				String smallType = tuple.getStringByField("cate_id3");
				String bigTypeName = tuple.getStringByField("cate_name1");
				String midTypeName = tuple.getStringByField("cate_name2");
				String smallTypeName = tuple.getStringByField("cate_name3");
				String itemTime = tuple.getStringByField("item_time");
				String text = tuple.getStringByField("text");
				
				if(!Utils.isBidValid(bid) || !Utils.isItemIdValid(itemId)){
					return;
				}
				
				if(debug){
					logger.info("input data, cate1="+bigTypeName+",cate2="+midTypeName+",cate3="+smallTypeName);
				}
				
				ByteString bigTypeNameStr = ByteString.copyFrom(bigTypeName,"UTF-8");
				ByteString midTypeNameStr = ByteString.copyFrom(midTypeName,"UTF-8");
				ByteString smallTypeNameStr = ByteString.copyFrom(smallTypeName,"UTF-8");
				ByteString textStr = ByteString.copyFrom(text,"UTF-8");
				
				
				
				
				NewsAttr.Builder cbAppBuilder = NewsAttr.newBuilder();
				genInputPbBuilder(cbAppBuilder,itemId,Long.valueOf(bigType),Long.valueOf(midType),Long.valueOf(smallType)
						,bigTypeNameStr,midTypeNameStr,smallTypeNameStr,textStr);
				
				cbAppBuilder.setFreshnessScore(Long.valueOf(itemTime));
				
				classifier.HierarchicalClassify(cbAppBuilder);
				
				if(debug){
					logger.info("get data from classifier");
				}
				
				for(NewsApp.Newsapp.NewsCategory cs:cbAppBuilder.getCategoryList()){
					String tag = cs.getName().toStringUtf8();
					if(cs.getName().isEmpty() || tag.equals("")){
						continue;
					}
					
					String tagKey = Utils.spliceStringBySymbol("#", bid,tag); 
					if(debug){
						logger.info("send change to index ,tagKey="+tagKey+",tag.size="+tag.length());
					}
					cbAppBuilder.setTagScore(cs.getWeight());
					
					new doReIndexsCallBack(tagKey,cbAppBuilder.build()).excute();
					
					
					String tagKey2 = Utils.spliceStringBySymbol("#", bid,"for_test"); 
					new doReIndexsCallBack(tagKey2,cbAppBuilder.build()).excute();
				}
				
				String inTdeKey = Utils.spliceStringBySymbol("#", bid,itemId);
				saveNewsAttrInTde(inTdeKey, cbAppBuilder.build());
				if(debug){
					logger.info("save attr in tde for cache ,inTdeKey="+inTdeKey);
				}
			}else if(topic.equals(Constants.actions_stream)){
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

	private void saveNewsAttrInTde(String key, NewsAttr value){
		for(ClientAttr clientEntry:mtClientList ){
			TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
			try {
				Future<Result<Void>> future = 
				clientEntry.getClient().putAsync((short)nsCbIndexTableId, 
									key.getBytes(), value.toByteArray(), putopt);
				clientEntry.getClient().notifyFuture(future, putCallBack, 
						new UpdateCallBackContext(clientEntry,key,value.toByteArray(),putopt));
			}catch(Exception e){
				logger.error(e.getMessage(), e);
			}
			break;
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
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() != null){
					UserFace qqFace = UserFace.parseFrom(res.getResult());					
					doRefreshUserFace(bidQQType,qqFace);
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}

		public void excute() {
			if(key == null || actionType == null){
				return;
			}
			
			UserFace qqFace = null;
			SoftReference<UserFace> sr = qqProfileCache.get(key);
			if(sr != null){
				qqFace = sr.get();
			}
				
			if(qqFace != null){
				doRefreshUserFace(bidQQType,qqFace);
			}else{
				try{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsUserFaceTableId,key.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}catch(Exception e){
					logger.error(e.getMessage(), e);
				}
			}
		}
		
		private void doRefreshUserFace(String bidQQType, UserFace qqFace){
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
					
					if(tag.equals("")){
						continue;
					}
														
					if(tag.equals(cate.getName().toStringUtf8())){
						UserFace.UserPreference.Builder newUpBuilder = 
								UserFace.UserPreference.newBuilder();
						
						float newWeight = up.getWeight() + computerWeight(1F ,0.25F ,actionType);
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
					
					float newWeight = computerWeight(1F ,0.25F ,actionType);
					
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
					 return (int)(arg1.getWeight() - arg0.getWeight());
				}
			}); 
			
			UserFace.Builder newQQFaceBuilder = UserFace.newBuilder();
			newQQFaceBuilder.setProfile(qqFace.getProfile());
			newQQFaceBuilder.addAllPreference(newQQFaceList);
			
			saveInTde(key,newQQFaceBuilder.build());
		}
		
		private float computerWeight(float level, float time_loss, String actionType){
			Float typeWeight = Utils.getActionWeight(Integer.valueOf(actionType));
			if(typeWeight != null){
				return level*time_loss*typeWeight;
			}else{
				return 0F;
			}
		}
		
		private void saveInTde(String key,UserFace value){
			synchronized(qqProfileCache){
				qqProfileCache.set(key, new SoftReference<UserFace>(value), cacheExpireTime);
			}
			
			if(debug && key.indexOf("389687043")>0){
				logger.info("user face,count="+value.getPreferenceCount());
			}
			
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
				try {
					Future<Result<Void>> future = 
					clientEntry.getClient().putAsync((short)nsUserFaceTableId, 
										this.key.getBytes(), value.toByteArray(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,key,value.toByteArray(),putopt));

					/*
					if(mt!=null){
						MonitorEntry mEntryPut = new MonitorEntry(Constants.SUCCESSCODE,Constants.SUCCESSCODE);
						mEntryPut.addExtField("TDW_IDC", clientEntry.getGroupname());
						mEntryPut.addExtField("tbl_name", "TopActions");
						mt.addCountEntry(Constants.systemID, Constants.tde_put_interfaceID, mEntryPut, 1);
					}*/
				} catch (Exception e){
					logger.error(e.getMessage(), e);
				}
			}
		}
	}
	
	public class doReIndexsCallBack implements MutiClientCallBack{
		private String tagKey;
		private NewsApp.Newsapp.NewsAttr itemAttr;
		
		public doReIndexsCallBack(String tagKey, NewsApp.Newsapp.NewsAttr itemAttr){
			this.tagKey = tagKey;
			this.itemAttr = itemAttr;
		}
		
		private void insertNewValueToList(NewsIndex oldNewsList, NewsIndex.Builder newListBuilder){
			HashSet<String> alreadyIn = new HashSet<String>();
			
			Long now = System.currentTimeMillis()/1000;
			newListBuilder.setCreateTime(now);
			newListBuilder.setUpdateTime(now);
			
			if(oldNewsList != null){
				newListBuilder.setCreateTime(oldNewsList.getCreateTime());
				for(NewsAttr eachNews :oldNewsList.getNewsListList()){
					if(newListBuilder.getNewsListCount() > topNum){
						break;
					}
															
					if(!alreadyIn.contains(itemAttr.getNewsId()) && itemAttr.getIndexScore() >= eachNews.getIndexScore()){
						newListBuilder.addNewsList(itemAttr);
						alreadyIn.add(itemAttr.getNewsId());
					}
						
					if(!alreadyIn.contains(eachNews.getNewsId()) 
							&& !eachNews.getNewsId().equals(itemAttr.getNewsId())){

						newListBuilder.addNewsList(eachNews);
						alreadyIn.add(eachNews.getNewsId());
					}
				}
			}
				
			if(!alreadyIn.contains(itemAttr.getNewsId())  
					&& newListBuilder.getNewsListCount() < topNum){
				newListBuilder.addNewsList(itemAttr);
				alreadyIn.add(itemAttr.getNewsId());
			}
		}
		
		private void save(String key, NewsIndex value){
			
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
				try {
					UpdateCallBack putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_send_interfaceID, key);
					Future<Result<Void>> future = clientEntry.getClient().putAsync((short)nsCbIndexTableId, 
										key.getBytes("UTF-8"), value.toByteArray(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,key,value.toByteArray(),putopt));
					
					
					/*if(mt!=null){
						MonitorEntry mEntryPut = new MonitorEntry(Constants.SUCCESSCODE,Constants.SUCCESSCODE);
						mEntryPut.addExtField("TDW_IDC", clientEntry.getGroupname());
						mEntryPut.addExtField("tbl_name", algName);
						mt.addCountEntry(Constants.systemID, Constants.tde_put_interfaceID, mEntryPut, 1);
					}*/
				} catch (Exception e){
					logger.error(e.getMessage(), e);
				}
			}
		}
		
		@Override
		public void handle(Future<?> future, Object context) {
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			NewsIndex oldNewsList = null;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() != null){
					oldNewsList = NewsIndex.parseFrom(res.getResult());
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}	
			
			NewsIndex.Builder newListBuilder = NewsIndex.newBuilder();
			insertNewValueToList(oldNewsList,newListBuilder);
			save(this.tagKey,newListBuilder.build());
		}

		public void excute() {
			try{
				ClientAttr clientEntry = mtClientList.get(0);		
				TairOption opt = new TairOption(clientEntry.getTimeout());
				Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsCbIndexTableId,tagKey.getBytes(),opt);
				clientEntry.getClient().notifyFuture(future, this,clientEntry);	
			}catch(Exception e){
				logger.error(e.getMessage(), e);
			}
		}

	}

	public static void main(String[] args){
		
		String bigType = "1";
		String midType = "2";
		String smallType = "3";
		String bigTypeName = "新闻";
		String midTypeName = "新闻";
		String smallTypeName = "要闻";
		String text = "门男子酒后强奸同村妇女##和他人喝酒后将同村一妇女强奸，潜逃11年后在吉林被抓获。##11年前，肖某因酒后强奸妇女潜逃，前日凌晨2时，天门市公安局九真派出所民警终将其从吉林省延吉市押回天门。  2003年3月2日晚，天门九真镇新河村村民肖某和陈某喝酒后将同村的一名妇女强奸。案发后，两人潜逃，陈某于2011年投案自首。20日，天门警方了解到肖某已潜逃至吉林省延吉市的信息，遂与延吉警方联系。经延吉警方多日蹲守，终将肖某抓获。";
		
		try {
			ByteString bigTypeNameStr = ByteString.copyFrom(bigTypeName,"UTF-8");
			ByteString midTypeNameStr = ByteString.copyFrom(midTypeName,"UTF-8");
			ByteString smallTypeNameStr = ByteString.copyFrom(smallTypeName,"UTF-8");
			ByteString textStr = ByteString.copyFrom(text,"UTF-8");
			
			NewsApp.Newsapp.NewsAttr.Builder cbAppBuilder = NewsApp.Newsapp.NewsAttr.newBuilder();
			genInputPbBuilder(cbAppBuilder,"1234",Long.valueOf(bigType),Long.valueOf(midType),Long.valueOf(smallType)
					,bigTypeNameStr,midTypeNameStr,smallTypeNameStr,textStr);
			
			NewsClassifier classifier = new NewsClassifier();
			classifier.HierarchicalClassify(cbAppBuilder);
			
			for(NewsCategory cs:cbAppBuilder.getCategoryList()){
				//ByteString smallTypeNameStr = ByteString.copyFrom(smallTypeName.getBytes());
				ByteString tag = cs.getName();
				String tagKey = Utils.spliceStringBySymbol("#", "10040001",tag.toStringUtf8()); 
	
				
				System.out.println(",csName="+cs.getName().toStringUtf8()
						+"\n,title="+cbAppBuilder.getTitle().toStringUtf8()
						+"\n,content="+cbAppBuilder.getContent().toStringUtf8()
						+"\n,tagKey="+tagKey+"\n\n\n");
			}
		} catch (UnsupportedEncodingException e1) {
			
			return;
		}
	
		
		Double simCF = (double) (1.5/(Math.sqrt(4.5) * Math.sqrt(540.5)));
		System.out.println("simCF="+simCF);
		
		String itemId = "20140322A000E7#1";
		itemId = itemId.substring(0,itemId.length()-2);
		System.out.print(itemId);
	}
	
}