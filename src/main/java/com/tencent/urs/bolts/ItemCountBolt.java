package com.tencent.urs.bolts;

import com.google.common.collect.ImmutableList;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.ActiveType;
import com.tencent.urs.protobuf.Recommend.GroupCountInfo;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType;
import com.tencent.urs.protobuf.Recommend.UserCountInfo;

import java.lang.ref.SoftReference;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.tencent.monitor.MonitorTools;

import com.tencent.streaming.commons.bolts.config.AbstractConfigUpdateBolt;
import com.tencent.streaming.commons.spouts.tdbank.Output;
import com.tencent.tde.client.Result;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.error.TairFlowLimit;
import com.tencent.tde.client.error.TairQueueOverflow;
import com.tencent.tde.client.error.TairRpcError;
import com.tencent.tde.client.impl.MutiThreadCallbackClient;
import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBackContext;
import com.tencent.urs.combine.GroupActionCombinerValue;
import com.tencent.urs.combine.UpdateKey;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.Utils;

public class ItemCountBolt extends AbstractConfigUpdateBolt{
	private static final long serialVersionUID = 2972911860800045348L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private DataCache<GroupCountInfo> groupCountCache;
	private DataCache<UserCountInfo> userCountCache;

	private ConcurrentHashMap<UpdateKey, GroupActionCombinerValue> combinerMap;
	private ConcurrentHashMap<Recommend.ActiveType, Float> actWeightMap;
	private Long lastUpdateTime; 
	private UpdateCallBack putCallBack;	
	
	private int nsUserCountTableId;
	private int nsGroupCountTableId;
	private int nsDetailTableId;
	private int nsActWeightTableId;
	
	private int dataExpireTime;
	private int cacheExpireTime;
	private boolean debug;
	
	public ItemCountBolt(String config, ImmutableList<Output> outputField) {
		super(config, outputField, Constants.config_stream);
	}
	
	public class MidInfo {
		private Long timeId;
		private Float weight;
		
		MidInfo(Long timeId,Float weight){
			this.timeId = timeId;
			this.weight = weight;
		}
		
		public Long getTimeId(){
			return this.timeId;
		}
		
		public Float getWeight(){
			return this.weight;
		}
	}

	@Override
	public void updateConfig(XMLConfiguration config) {
		nsUserCountTableId = config.getInt("user_count_table",303);
		nsGroupCountTableId = config.getInt("group_count_table",304);
		nsDetailTableId = config.getInt("dependent_table",302);
		
		nsActWeightTableId = config.getInt("act_weight_table",314);
		dataExpireTime = config.getInt("data_expiretime",1*24*3600);
		cacheExpireTime = config.getInt("cache_expiretime",3600);
		debug = config.getBoolean("debug",false);
	}
	
	private void weightInit(){
		actWeightMap.put(Recommend.ActiveType.Impress, 0.5F);
		actWeightMap.put(Recommend.ActiveType.Click, 1F);
		actWeightMap.put(Recommend.ActiveType.PageView, 1F);
		actWeightMap.put(Recommend.ActiveType.Read, 1.5F);
		actWeightMap.put(Recommend.ActiveType.Save, 2F);
		actWeightMap.put(Recommend.ActiveType.BuyCart, 2F);
		actWeightMap.put(Recommend.ActiveType.Deal, 2F);
		actWeightMap.put(Recommend.ActiveType.Score, 3F);
		actWeightMap.put(Recommend.ActiveType.Comments, 3F);
		actWeightMap.put(Recommend.ActiveType.Reply, 3F);
		actWeightMap.put(Recommend.ActiveType.Ups, 3F);
		actWeightMap.put(Recommend.ActiveType.Praise, 4F);
		actWeightMap.put(Recommend.ActiveType.Share, 4F);
	}
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		super.prepare(conf, context, collector);
		updateConfig(super.config);
	
		this.lastUpdateTime = 0L;
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.groupCountCache = new DataCache<GroupCountInfo>(conf);
		this.userCountCache = new DataCache<UserCountInfo>(conf);
		
		this.actWeightMap = new ConcurrentHashMap<Recommend.ActiveType, Float>(20);
		
		this.combinerMap = new ConcurrentHashMap<UpdateKey,GroupActionCombinerValue>(1024);
				
		this.putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_interfaceID, this.getClass().getName());
		
		int combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5);
		setCombinerTime(combinerExpireTime);
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {	
		String bid = tuple.getStringByField("bid");
		String qq = tuple.getStringByField("qq");
		String groupId = tuple.getStringByField("group_id");
		String adpos = tuple.getStringByField("adpos");
		String itemId = tuple.getStringByField("item_id");
		
		String actionType = tuple.getStringByField("action_type");
		String actionTime = tuple.getStringByField("action_time");
	
		ActiveType actType = Utils.getActionTypeByString(actionType);
		
		if(!Utils.isBidValid(bid) || !Utils.isQNumValid(qq) || !Utils.isGroupIdVaild(groupId) || !Utils.isItemIdValid(itemId)){
			return;
		}
		
		GroupActionCombinerValue value = new GroupActionCombinerValue(actType,Long.valueOf(actionTime));
		UpdateKey key = new UpdateKey(bid,Long.valueOf(qq),Integer.valueOf(groupId),adpos,itemId);
		combinerKeys(key,value);
	}
	
	private static Logger logger = LoggerFactory
			.getLogger(ItemCountBolt.class);
	
	private void setCombinerTime(final int second) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					while (true) {
						Thread.sleep(second * 1000);
						Set<UpdateKey> keySet = combinerMap.keySet();
						for (UpdateKey key : keySet) {
							GroupActionCombinerValue expireTimeValue  = combinerMap.remove(key);
							try{
								new ActionDetailCallBack(key,expireTimeValue).excute();
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
	
	private void combinerKeys(UpdateKey key,GroupActionCombinerValue value) {		
		synchronized(combinerMap){
			if(combinerMap.containsKey(key)){
				GroupActionCombinerValue oldValue = combinerMap.get(key);
				oldValue.incrument(value);
				combinerMap.put(key, oldValue);				
			}else{
				combinerMap.put(key, value);			
			}
		}
	}	
	
	private Float getWeightByType(String bid,Recommend.ActiveType actionType){
		
		/*Long now = System.currentTimeMillis()/1000L;
		if(lastUpdateTime == 0){
			weightInit();
			lastUpdateTime = now;
		}else if(lastUpdateTime < (now - 3600*24)  ){
			actWeightMap.clear();
			lastUpdateTime = now;
		}*/
		
		//String actTypeKey = this.
		weightInit();		
		if(actWeightMap.containsKey(actionType)){
			return actWeightMap.get(actionType);
		}else{
			try{
				MutiThreadCallbackClient clientEntry = mtClientList.get(0).getClient();
				TairOption opt = new TairOption(mtClientList.get(0).getTimeout());
				Result<byte[]> res =  clientEntry.get((short)nsActWeightTableId, actionType.toString().getBytes(), opt);
				if(res.isSuccess() && res.getResult()!=null)
				{
						
					Recommend.ActionWeightInfo pbWeightInfo = Recommend.ActionWeightInfo.parseFrom(res.getResult());
					if(pbWeightInfo.getWeight()>=0){
						actWeightMap.put(actionType, pbWeightInfo.getWeight());
						return pbWeightInfo.getWeight();
					}
				}					
			}catch(Exception e){
				logger.error(e.toString());
			}
		}
		return 0F;
	}

	private class GroupCountUpdateCallback implements MutiClientCallBack{
		private final MidInfo weightInfo;
		private final String groupCountKey;

		public GroupCountUpdateCallback(UpdateKey key, MidInfo weightInfo) {
			this.weightInfo = weightInfo;		
			this.groupCountKey = key.getGroupCountKey();
		}
		
		public void excute() {
			try {			
				GroupCountInfo oldWeightInfo = null;
				SoftReference<GroupCountInfo> sr = groupCountCache.get(groupCountKey);
				if(sr != null){
					oldWeightInfo = sr.get();
				}
				
				if(oldWeightInfo != null){		
					next(oldWeightInfo);
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsGroupCountTableId,groupCountKey.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}			
				
			} catch (Exception e){
				logger.error(e.getMessage(), e);
			}
		}

		private void save(String key,GroupCountInfo value){		
			synchronized(groupCountCache){
				groupCountCache.set(key, new SoftReference<GroupCountInfo>(value), cacheExpireTime);
			}
			
			logger.info("step3 save pair count,key="+key+",value="+value.getTsegsCount());
			Future<Result<Void>> future = null;
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
				try {
					future = clientEntry.getClient().putAsync((short)nsGroupCountTableId, key.getBytes(),value.toByteArray(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,key,value.toByteArray(),putopt));
					
					/*
					if(mt!=null){
						MonitorEntry mEntryPut = new MonitorEntry(Constants.SUCCESSCODE,Constants.SUCCESSCODE);
						mEntryPut.addExtField("TDW_IDC", clientEntry.getGroupname());
						mEntryPut.addExtField("tbl_name", "FIFO1");
						mt.addCountEntry(Constants.systemID, Constants.tde_put_interfaceID, mEntryPut, 1);
					}*/
				} catch (Exception e){
					logger.error(e.getMessage(), e);
				}
			}
		}
		
		@Override
		public void handle(Future<?> future, Object context) {
			// TODO Auto-generated method stub
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			Recommend.GroupCountInfo oldWeightInfo = null;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() != null){
					oldWeightInfo = Recommend.GroupCountInfo.parseFrom(res.getResult());
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
			next(oldWeightInfo);
		}
		
		public void next(GroupCountInfo oldWeightInfo){
			//logger.info("get step2 key="+userCountKey+",old count="+oldCount+",new count="+count);
			
			GroupCountInfo.Builder newGroupInfoBuiler = GroupCountInfo.newBuilder();
			boolean isAdded = false;
			if(oldWeightInfo != null){
				for(GroupCountInfo.TimeSegment ts:oldWeightInfo.getTsegsList()){
					if(ts.getTimeId() < weightInfo.getTimeId() - 7){
						continue;
					}
					
					Float newCount = 0F;
					if(ts.getTimeId() == weightInfo.getTimeId()){
						newCount = ts.getCount() + weightInfo.getWeight();	
						
						GroupCountInfo.TimeSegment.Builder tsBuilder = GroupCountInfo.TimeSegment.newBuilder();
						tsBuilder.setTimeId(ts.getTimeId()).setCount(newCount);
						newGroupInfoBuiler.addTsegs(tsBuilder.build());
						isAdded = true;
					}else {
						newGroupInfoBuiler.addTsegs(ts);
					}
				}
			}
			
			if(!isAdded){
				GroupCountInfo.TimeSegment.Builder tsBuilder = GroupCountInfo.TimeSegment.newBuilder();
				tsBuilder.setTimeId(weightInfo.getTimeId()).setCount(weightInfo.getWeight());
				newGroupInfoBuiler.addTsegs(tsBuilder.build());
			}

			save(groupCountKey,newGroupInfoBuiler.build());
		
		}
				
	}
	
	private class UserCountUpdateCallBack implements MutiClientCallBack{
		
		private UpdateKey key;
		private MidInfo weightInfo;
		private String userCountKey;

		public UserCountUpdateCallBack(UpdateKey key, MidInfo weightInfo) {
			this.key = key ; 
			this.weightInfo = weightInfo;	
			this.userCountKey = key.getUserCountKey();
		}
		
		public void excute() {
			try {
				UserCountInfo oldWeightInfo = null;
				SoftReference<UserCountInfo> sr = userCountCache.get(userCountKey);
				if(sr != null){
					oldWeightInfo = sr.get();
				}
				
				if(oldWeightInfo != null){		
					next(oldWeightInfo);
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsUserCountTableId,userCountKey.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}			
				
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
		
		public void next(UserCountInfo oldWeightInfo){
			//logger.info("get step2 key="+userCountKey+",old count="+oldCount+",new count="+count);
			
			UserCountInfo.Builder newUserInfoBuiler = UserCountInfo.newBuilder();
			boolean isAdded = false;
			if(oldWeightInfo != null){
				for(UserCountInfo.TimeSegment ts:oldWeightInfo.getTsegsList()){
					if(ts.getTimeId() < weightInfo.getTimeId() - 7){
						continue;
					}
					
					Float newCount = 0F;
					if(ts.getTimeId() == weightInfo.getTimeId()){
						Float changeWeight = weightInfo.getWeight()-ts.getCount();
						MidInfo newMidInfo = new MidInfo(ts.getTimeId(), changeWeight);
						new GroupCountUpdateCallback(key,newMidInfo).excute();
						
						UpdateKey noGroupKey = new UpdateKey(key.getBid(),key.getUin(),0,key.getAdpos(),key.getItemId());
						new GroupCountUpdateCallback(noGroupKey,newMidInfo).excute();
						
						newCount = ts.getCount();
						
						isAdded = true;
					}else if(ts.getCount() >= 0){
						Float changeWeight = 0 - ts.getCount();
						MidInfo newMidInfo = new MidInfo(ts.getTimeId(), changeWeight);
						
						new GroupCountUpdateCallback(key,newMidInfo).excute();
							
						UpdateKey noGroupKey = new UpdateKey(key.getBid(),key.getUin(),0,key.getAdpos(),key.getItemId());
						new GroupCountUpdateCallback(noGroupKey,newMidInfo).excute();
					}
					
					UserCountInfo.TimeSegment.Builder tsBuilder = UserCountInfo.TimeSegment.newBuilder();
					tsBuilder.setTimeId(ts.getTimeId()).setCount(newCount);
					newUserInfoBuiler.addTsegs(tsBuilder.build());
				}
			}
			
			if(!isAdded){
				UserCountInfo.TimeSegment.Builder tsBuilder = UserCountInfo.TimeSegment.newBuilder();
				tsBuilder.setTimeId(weightInfo.getTimeId()).setCount(weightInfo.getWeight());
				newUserInfoBuiler.addTsegs(tsBuilder.build());
			}

			save(userCountKey,newUserInfoBuiler.build());		
		}
		
		private void save(String userCountKey,UserCountInfo newWeightInfo){
			Future<Result<Void>> future = null;
			synchronized(userCountCache){
				userCountCache.set(userCountKey, new SoftReference<UserCountInfo>(newWeightInfo), cacheExpireTime);
			}
			
			logger.info("step2,save usercount,key="+userCountKey+",value=");
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
				try {
					future = clientEntry.getClient().putAsync((short)nsUserCountTableId, userCountKey.getBytes(),newWeightInfo.toByteArray(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,userCountKey,newWeightInfo.toByteArray(),putopt));
					
					/*
					if(mt!=null){
						MonitorEntry mEntryPut = new MonitorEntry(Constants.SUCCESSCODE,Constants.SUCCESSCODE);
						mEntryPut.addExtField("TDW_IDC", clientEntry.getGroupname());
						mEntryPut.addExtField("tbl_name", "FIFO1");
						mt.addCountEntry(Constants.systemID, Constants.tde_put_interfaceID, mEntryPut, 1);
					}*/
				} catch (Exception e){
					logger.error(e.getMessage(), e);
				}
			}
		}
			
		@Override
		public void handle(Future<?> future, Object arg1) {
			// TODO Auto-generated method stub
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			UserCountInfo oldWeightInfo = null;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() != null){
					oldWeightInfo = UserCountInfo.parseFrom(res.getResult());
				}				
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
			next(oldWeightInfo);	
		}
		
	}
	
	private class ActionDetailCallBack implements MutiClientCallBack{
		private final UpdateKey key;
		private final String userCheckKey;
		private final GroupActionCombinerValue values;

		public ActionDetailCallBack(UpdateKey key, GroupActionCombinerValue values) {
			this.key = key ; 
			this.values = values;		
			this.userCheckKey = key.getDetailKey();
		}

		public void next(MidInfo weightInfo){
			if(weightInfo.getWeight() > 0){
				new UserCountUpdateCallBack(key,weightInfo).excute();
			}
		}
		
		public void excute() {
			try {
				ClientAttr clientEntry = mtClientList.get(0);		
				TairOption opt = new TairOption(clientEntry.getTimeout());
				Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsDetailTableId,userCheckKey.getBytes(),opt);
				clientEntry.getClient().notifyFuture(future, this,clientEntry);	
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
			
		private MidInfo getMaxWeight(Recommend.UserActiveDetail oldValueHeap){						
			Float newWeight = getWeightByType(key.getBid(),this.values.getType());		
			Long timeId = Utils.getDateByTime(values.getTime());
			for(TimeSegment ts:oldValueHeap.getTsegsList()){
				if(ts.getTimeId() <  Utils.getDateByTime(values.getTime() - dataExpireTime)){
					continue;
				}
			
				for(ItemInfo item:ts.getItemsList()){						
					if(item.getItem().equals(key.getItemId())){	
						for(ActType act: item.getActsList()){	
							if(act.getLastUpdateTime() > (values.getTime() - dataExpireTime)){
								Float actWeight = getWeightByType(key.getBid(),act.getActType());
								if(actWeight > newWeight){
									newWeight =  Math.max(newWeight,actWeight);
									timeId = ts.getTimeId();
									logger.info("timeId="+ts.getTimeId()+",type="+act.getActType()+",weight="+newWeight);
								}
							}
						}	
					}					
				}
			}
			logger.info("item="+key.getItemId()+",final weight="+newWeight);
			return new MidInfo(timeId,newWeight);
		}
		
		@Override
		public void handle(Future<?> future, Object context) {			
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() !=null){
					Recommend.UserActiveDetail oldValueHeap = Recommend.UserActiveDetail.parseFrom(res.getResult());
					next(getMaxWeight(oldValueHeap));
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}	
		}
	}
	
}