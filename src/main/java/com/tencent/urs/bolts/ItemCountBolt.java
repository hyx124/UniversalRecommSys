package com.tencent.urs.bolts;

import com.google.common.collect.ImmutableList;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.GroupCountInfo;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType;
import com.tencent.urs.protobuf.Recommend.UserCountInfo;

import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.HashSet;
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
	private ConcurrentHashMap<Integer, Float> actWeightMap;
	private UpdateCallBack putCallBack;	
	
	private int nsUserCountTableId;
	private int nsGroupCountTableId;
	private int nsDetailTableId;
	
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
		nsUserCountTableId = config.getInt("user_count_table",513);
		nsGroupCountTableId = config.getInt("group_count_table",514);
		nsDetailTableId = config.getInt("dependent_table",512);
		
		dataExpireTime = config.getInt("data_expiretime",7*24*3600);
		cacheExpireTime = config.getInt("cache_expiretime",3600);
		debug = config.getBoolean("debug",false);
	}
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		super.prepare(conf, context, collector);
		updateConfig(super.config);
	
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.groupCountCache = new DataCache<GroupCountInfo>(conf);
		this.userCountCache = new DataCache<UserCountInfo>(conf);
		
		this.actWeightMap = new ConcurrentHashMap<Integer, Float>(20);
		
		this.combinerMap = new ConcurrentHashMap<UpdateKey,GroupActionCombinerValue>(1024);
				
		this.putCallBack = new UpdateCallBack(mt, this.nsUserCountTableId ,debug);
		
		int combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5);
		setCombinerTime(combinerExpireTime);
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {	
		try{
			
			String bid = tuple.getStringByField("bid");
			String qq = tuple.getStringByField("qq");
			String groupId = tuple.getStringByField("group_id");
			String adpos = Constants.DEFAULT_ADPOS;
			String itemId = tuple.getStringByField("item_id");
			
			String actionType = tuple.getStringByField("action_type");
			String actionTime = tuple.getStringByField("action_time");
					
			if(!Utils.isBidValid(bid) || !Utils.isQNumValid(qq) 
					|| !Utils.isGroupIdVaild(groupId) || !Utils.isItemIdValid(itemId)){
				return;
			}
			
			GroupActionCombinerValue value = 
					new GroupActionCombinerValue(Integer.valueOf(actionType),Long.valueOf(actionTime));
			UpdateKey key = new UpdateKey(bid,Long.valueOf(qq),Integer.valueOf(groupId),adpos,itemId);
			combinerKeys(key,value);
		}catch(Exception e){
			logger.error(e.getMessage(), e);
		}

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
	
	private Float getWeightByType(String bid,Integer actionType){
		return Utils.getActionWeight(actionType);
	}

	private class GroupCountUpdateCallback implements MutiClientCallBack{
		private final HashMap<Long,MidInfo> weightInfoMap;
		private final String groupCountKey;
		private final UpdateKey inKey;

		public GroupCountUpdateCallback(UpdateKey key, HashMap<Long,MidInfo> weightInfoMap) {
			this.weightInfoMap = weightInfoMap;		
			this.groupCountKey = key.getGroupCountKey();
			this.inKey = key;
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
			if(debug){
				for(GroupCountInfo.TimeSegment ts:value.getTsegsList()){
					logger.info("step3,save loop,key="+groupCountKey+",date="+ts.getTimeId()+",save Weight="+ts.getCount());
				}				
			}
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
				break;
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
			HashSet<Long> alreadyIn = new HashSet<Long>();
			Long now = System.currentTimeMillis()/1000;
			if(oldWeightInfo != null){
				for(GroupCountInfo.TimeSegment ts:oldWeightInfo.getTsegsList()){
					if(ts.getTimeId() < Utils.getDateByTime( now - dataExpireTime)){
						continue;
					}
					
					Float newCount = 0F;
					if(weightInfoMap.containsKey(ts.getTimeId())){
						newCount = ts.getCount() + weightInfoMap.get(ts.getTimeId()).getWeight();
						if(debug){
							logger.info("step3,add changes to group,item="+inKey.getItemId()+",date="+ts.getTimeId()+",oldWeight="+ts.getCount()+",change="+weightInfoMap.get(ts.getTimeId()).getWeight()+",newWeight="+newCount);
						}
						
						GroupCountInfo.TimeSegment.Builder tsBuilder = GroupCountInfo.TimeSegment.newBuilder();
						tsBuilder.setTimeId(ts.getTimeId()).setCount(newCount);
						newGroupInfoBuiler.addTsegs(tsBuilder.build());
					}else{
						if(debug){
							logger.info("step3 ,not found group new count,item="+inKey.getItemId()+",date="+ts.getTimeId()+",oldWeight="+ts.getCount());
						}
						newGroupInfoBuiler.addTsegs(ts);
					}
					
					alreadyIn.add(ts.getTimeId());
				}
			}else{
				if(debug){
					logger.info("step3 ,old heap is null");
				}
			}
			
			for(Long key: weightInfoMap.keySet()){
				if(!alreadyIn.contains(key)){
					GroupCountInfo.TimeSegment.Builder tsBuilder = GroupCountInfo.TimeSegment.newBuilder();
					tsBuilder.setTimeId(key).setCount(weightInfoMap.get(key).getWeight());
					
					if(debug){
						logger.info("step3,not found old ,addd new,item="+inKey.getItemId() +",date="+key+",weight="+weightInfoMap.get(key).getWeight());
					}
					
					newGroupInfoBuiler.addTsegs(0,tsBuilder.build());
					alreadyIn.add(key);
				}
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
			HashMap<Long,MidInfo> midInfoMap = new HashMap<Long,MidInfo>();
			boolean isAdded = false;
			if(oldWeightInfo != null){
				if(debug){
					for(UserCountInfo.TimeSegment ts:oldWeightInfo.getTsegsList()){
						logger.info("step2,loop,itemId="+key.getItemId()+",qq="+key.getUin()+",date="+ts.getTimeId()+",old="+ts.getCount());
					}
				}
				
				
				for(UserCountInfo.TimeSegment ts:oldWeightInfo.getTsegsList()){
					if(ts.getTimeId() < weightInfo.getTimeId() - dataExpireTime/24/3600){
						continue;
					}
					
					Float newCount = 0F;
					if(ts.getTimeId() == weightInfo.getTimeId()){
						Float changeWeight = weightInfo.getWeight()-ts.getCount();
						MidInfo newMidInfo = new MidInfo(ts.getTimeId(), changeWeight);
						
						if(debug){
							logger.info("step2,same date,itemId="+key.getItemId()+",qq="+key.getUin()+",date="+newMidInfo.getTimeId()+",changeWeight="+newMidInfo.getWeight());
						}
						
						if(changeWeight != 0){
							midInfoMap.put(ts.getTimeId(),newMidInfo);
						}
						
						newCount = weightInfo.getWeight();
						isAdded = true;
					}else if(ts.getCount() >= 0){
						Float changeWeight = 0 - ts.getCount();
						MidInfo newMidInfo = new MidInfo(ts.getTimeId(), changeWeight);
						
						if(debug){
							logger.info("step2,other date,itemId="+key.getItemId()+",qq="+key.getUin()+",date="+newMidInfo.getTimeId()+",changeWeight="+newMidInfo.getWeight());
						}
						
						if(changeWeight != 0){
							midInfoMap.put(ts.getTimeId(),newMidInfo);
						}
						
						newCount = 0F;
					}
					
					UserCountInfo.TimeSegment.Builder tsBuilder = UserCountInfo.TimeSegment.newBuilder();
					tsBuilder.setTimeId(ts.getTimeId()).setCount(newCount);
					newUserInfoBuiler.addTsegs(tsBuilder.build());
				}
			}
			
			if(!isAdded){
				Float changeWeight = weightInfo.getWeight();
				MidInfo newMidInfo = new MidInfo(weightInfo.getTimeId(), changeWeight);
				
				if(debug){
					logger.info("step2, not found old date,add new ,itemId="+key.getItemId()+",qq="+key.getUin());
				}
				
				if(changeWeight != 0){
					midInfoMap.put(weightInfo.getTimeId(),newMidInfo);
				}
				
				UserCountInfo.TimeSegment.Builder tsBuilder = UserCountInfo.TimeSegment.newBuilder();
				tsBuilder.setTimeId(newMidInfo.getTimeId()).setCount(newMidInfo.getWeight());
				newUserInfoBuiler.addTsegs(0,tsBuilder.build());
								
				isAdded = true;
			}

			save(userCountKey,newUserInfoBuiler.build());	
			
			new GroupCountUpdateCallback(key,midInfoMap).excute();
			
			if(key.getGroupId() != 0){
				UpdateKey noGroupKey = new UpdateKey(key.getBid(),key.getUin(),0,key.getAdpos(),key.getItemId());
				new GroupCountUpdateCallback(noGroupKey,midInfoMap).excute();
			}
		}
		
		private void save(String userCountKey,UserCountInfo newWeightInfo){
			Future<Result<Void>> future = null;
			synchronized(userCountCache){
				userCountCache.set(userCountKey, new SoftReference<UserCountInfo>(newWeightInfo), cacheExpireTime);
			}
			if(debug){
				for(UserCountInfo.TimeSegment ts:newWeightInfo.getTsegsList()){
					logger.info("step2,save loop,itemId="+key.getItemId()+",qq="+key.getUin()+",date="+ts.getTimeId()+",old="+ts.getCount());
				}
			}
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
				break;
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
			new UserCountUpdateCallBack(key,weightInfo).excute();
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
							Float actWeight = getWeightByType(key.getBid(),act.getActType());
							if(actWeight > newWeight){
								newWeight =  Math.max(newWeight,actWeight);
								timeId = ts.getTimeId();
							}
						}	
					}					
				}
			}
			if(debug){
				logger.info("step1,item="+key.getItemId()+",qq="+key.getUin()+",final weight="+newWeight);
			}
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