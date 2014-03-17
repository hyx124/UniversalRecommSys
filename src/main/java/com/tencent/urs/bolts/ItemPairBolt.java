package com.tencent.urs.bolts;

import com.google.common.collect.ImmutableList;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.ActiveType;
import com.tencent.urs.protobuf.Recommend.GroupPairInfo;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType;
import com.tencent.urs.protobuf.Recommend.UserPairInfo;

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



public class ItemPairBolt  extends AbstractConfigUpdateBolt{
	private static final long serialVersionUID = -3578535683081183276L;
	private List<ClientAttr> mtClientList;	
	private Long lastUpdateTime; 
	private MonitorTools mt;
	private DataCache<Recommend.GroupPairInfo> groupPairCache;
	private DataCache<Recommend.UserPairInfo> userPairCache;
	private UpdateCallBack putCallBack;
	private ConcurrentHashMap<UpdateKey, GroupActionCombinerValue> combinerMap;
	private ConcurrentHashMap<Recommend.ActiveType, Float> actWeightMap;

	

	private int nsDetailTableId;
	private int dataExpireTime;
	private int cacheExpireTime;
	private int nsUserPairTableId;
	private int nsGroupPairTableId;
	private int nsActWeightTableId;
	
	private boolean debug;
	
	private static Logger logger = LoggerFactory
			.getLogger(ItemPairBolt.class);
	
	public ItemPairBolt(String config, ImmutableList<Output> outputField){
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
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		super.prepare(conf, context, collector);
		this.updateConfig(super.config);

		this.groupPairCache = new DataCache<Recommend.GroupPairInfo>(conf);
		this.userPairCache = new DataCache<Recommend.UserPairInfo>(conf);
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.combinerMap = new ConcurrentHashMap<UpdateKey,GroupActionCombinerValue>(1024);
				
		this.lastUpdateTime = 0L;
		this.actWeightMap = new ConcurrentHashMap<Recommend.ActiveType, Float>(20);
		
		this.putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_interfaceID, this.getClass().getName());
		
		int combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5);
		setCombinerTime(combinerExpireTime);
	}	


	@Override
	public void updateConfig(XMLConfiguration config) {
		nsUserPairTableId = config.getInt("user_pair_table",513);
		nsGroupPairTableId = config.getInt("group_pair_table",514);
		nsDetailTableId = config.getInt("dependent_table",512);
		dataExpireTime = config.getInt("data_expiretime",7*24*3600);
		cacheExpireTime = config.getInt("cache_expiretime",3600);
		nsActWeightTableId = config.getInt("act_weight_table",524);
		
		debug = config.getBoolean("debug",false);
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {
		// TODO Auto-generated method stub	
		try{
			String bid = tuple.getStringByField("bid");
			String qq = tuple.getStringByField("qq");
			String groupId = tuple.getStringByField("group_id");
			String adpos = Constants.DEFAULT_ADPOS;
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
		}catch(Exception e){
			logger.error(e.getMessage(), e);
		}
			
	}
	
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
					logger.error(e.getMessage(), e);
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
	
	private Float getWeightByType(Recommend.ActiveType actionType){
		
		/*Long now = System.currentTimeMillis()/1000L;
		if(lastUpdateTime == 0){
			weightInit();
			lastUpdateTime = now;
		}else if(lastUpdateTime < (now - 3600*24)  ){
			actWeightMap.clear();
			lastUpdateTime = now;
		}*/
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
				logger.error(e.getMessage(), e);
			}
		}
		return 0F;
	}

	private class GroupPairUpdateCallback implements MutiClientCallBack{
		private final UpdateKey key;
		private final String putKey;
		private final HashMap<Long,MidInfo> weightInfoMap;

		public GroupPairUpdateCallback(UpdateKey key, String otherItem, HashMap<Long,MidInfo> weightInfoMap) {
			this.key = key ; 
			this.weightInfoMap = weightInfoMap;
			this.putKey = key.getGroupPairKey(otherItem);
		}
		
		public void excute() {
			try {
				GroupPairInfo oldInfo = null;
				SoftReference<GroupPairInfo> sr = groupPairCache.get(putKey);
				if(sr != null){
					oldInfo = sr.get();
				}
				
				if(oldInfo != null){		
					next(oldInfo);
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsDetailTableId,putKey.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}			
				
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}

		private void save(String key,GroupPairInfo newInfo){	
		
			synchronized(groupPairCache){
				groupPairCache.set(key, new SoftReference<Recommend.GroupPairInfo>(newInfo), cacheExpireTime);
			}
			
			Future<Result<Void>> future = null;
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
				try {
					future = clientEntry.getClient().putAsync((short)nsGroupPairTableId, key.getBytes(),newInfo.toByteArray(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,key,newInfo.toByteArray(),putopt));
					
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
			GroupPairInfo.Builder newInfoBuilder = GroupPairInfo.newBuilder();
			GroupPairInfo oldInfo = null;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() != null){
					oldInfo	= GroupPairInfo.parseFrom(res.getResult());	
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);	
			}
			next(oldInfo);
		}
		
		public void next(GroupPairInfo oldWeightInfo){
			//logger.info("get step2 key="+userCountKey+",old count="+oldCount+",new count="+count);
			
			GroupPairInfo.Builder newGroupInfoBuiler = GroupPairInfo.newBuilder();
			HashSet<Long> alreadyIn = new HashSet<Long>();
			Long now = System.currentTimeMillis()/1000;
			if(oldWeightInfo != null){
				for(GroupPairInfo.TimeSegment ts:oldWeightInfo.getTsegsList()){
					if(ts.getTimeId() < Utils.getDateByTime( now - dataExpireTime)){
						continue;
					}
					
					Float newCount = 0F;
					if(weightInfoMap.containsKey(ts.getTimeId())){
						newCount = ts.getCount() + weightInfoMap.get(ts.getTimeId()).getWeight();
						if(debug && key.getUin() == 389687043L){
							logger.info("step3,add changes to group,date="+ts.getTimeId()+",oldWeight="+ts.getCount()+",change="+weightInfoMap.get(ts.getTimeId()).getWeight()+",newWeight="+newCount);
						}
						
						GroupPairInfo.TimeSegment.Builder tsBuilder = GroupPairInfo.TimeSegment.newBuilder();
						tsBuilder.setTimeId(ts.getTimeId()).setCount(newCount);
						newGroupInfoBuiler.addTsegs(tsBuilder.build());
					}else{
						if(debug && key.getUin() == 389687043L){
							logger.info("step3 ,not found group new count,date="+ts.getTimeId()+",oldWeight="+ts.getCount());
						}
						newGroupInfoBuiler.addTsegs(ts);
					}
					
					alreadyIn.add(ts.getTimeId());
				}
			}else{
				if(debug && key.getUin() == 389687043L){
					logger.info("step3 ,old heap is null");
				}
			}
			
			for(Long key: weightInfoMap.keySet()){
				if(!alreadyIn.contains(key)){
					GroupPairInfo.TimeSegment.Builder tsBuilder = GroupPairInfo.TimeSegment.newBuilder();
					tsBuilder.setTimeId(key).setCount(weightInfoMap.get(key).getWeight());
					
					if(debug && this.key.getUin() == 389687043L){
						logger.info("step3,not found old ,addd new,date="+key+",weight="+weightInfoMap.get(key).getWeight());
					}
					
					newGroupInfoBuiler.addTsegs(0,tsBuilder.build());
					alreadyIn.add(key);
				}
			}
			save(putKey,newGroupInfoBuiler.build());
		}

		
	}
	
	private class UserPairUpdateCallBack implements MutiClientCallBack{
		
		private UpdateKey key;
		private MidInfo weightInfo;
		private String userPairKey;
		private String itemId;

		public UserPairUpdateCallBack(UpdateKey key,String otherItem, MidInfo weightInfo) {
			this.key = key ; 
			this.weightInfo = weightInfo;	
			this.itemId = otherItem;
			
			this.userPairKey = key.getUserPairKey(otherItem);
		}
		
		public void excute() {
			try {
				Recommend.UserPairInfo oldInfo = null;
				SoftReference<Recommend.UserPairInfo> sr = userPairCache.get(userPairKey);	
				if(sr != null){
					oldInfo = sr.get();
				}
				
				if(oldInfo != null){	
					next(oldInfo);
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsUserPairTableId,userPairKey.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}			
				
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
		
		private void next(UserPairInfo oldWeightInfo){
			//logger.info("get step2 key="+userCountKey+",old count="+oldCount+",new count="+count);
			
			UserPairInfo.Builder newUserInfoBuiler = UserPairInfo.newBuilder();
			HashMap<Long,MidInfo> midInfoMap = new HashMap<Long,MidInfo>();
			boolean isAdded = false;
			if(oldWeightInfo != null){
				if(debug && key.getUin() == 389687043L){
					for(UserPairInfo.TimeSegment ts:oldWeightInfo.getTsegsList()){
						logger.info("step2,loop,key="+userPairKey+",date="+ts.getTimeId()+",old="+ts.getCount());
					}
				}
				
				
				for(UserPairInfo.TimeSegment ts:oldWeightInfo.getTsegsList()){
					if(ts.getTimeId() < weightInfo.getTimeId() - dataExpireTime/24/3600){
						continue;
					}
					
					Float newCount = 0F;
					if(ts.getTimeId() == weightInfo.getTimeId()){
						Float changeWeight = weightInfo.getWeight()-ts.getCount();
						MidInfo newMidInfo = new MidInfo(ts.getTimeId(), changeWeight);
						
						if(debug && key.getUin() == 389687043L){
							logger.info("step2,same date,key="+userPairKey+",date="+newMidInfo.getTimeId()+",changeWeight="+newMidInfo.getWeight());
						}
						
						if(changeWeight != 0){
							midInfoMap.put(ts.getTimeId(),newMidInfo);
						}
						
						newCount = weightInfo.getWeight();
						isAdded = true;
					}else if(ts.getCount() >= 0){
						Float changeWeight = 0 - ts.getCount();
						MidInfo newMidInfo = new MidInfo(ts.getTimeId(), changeWeight);
						
						if(debug && key.getUin() == 389687043L){
							logger.info("step2,other date,key="+userPairKey+",date="+newMidInfo.getTimeId()+",changeWeight="+newMidInfo.getWeight());
						}
						
						if(changeWeight != 0){
							midInfoMap.put(ts.getTimeId(),newMidInfo);
						}
						
						newCount = 0F;
					}
					
					UserPairInfo.TimeSegment.Builder tsBuilder = UserPairInfo.TimeSegment.newBuilder();
					tsBuilder.setTimeId(ts.getTimeId()).setCount(newCount);
					newUserInfoBuiler.addTsegs(tsBuilder.build());
				}
			}
			
			if(!isAdded){
				Float changeWeight = weightInfo.getWeight();
				MidInfo newMidInfo = new MidInfo(weightInfo.getTimeId(), changeWeight);
				
				if(debug && key.getUin() == 389687043L){
					logger.info("step2, not found old date,key="+userPairKey+",new date="+newMidInfo.getTimeId()+",new weight="+changeWeight);
				}
				
				if(changeWeight > 0){
					midInfoMap.put(weightInfo.getTimeId(),newMidInfo);
				}
				
				UserPairInfo.TimeSegment.Builder tsBuilder = UserPairInfo.TimeSegment.newBuilder();
				tsBuilder.setTimeId(newMidInfo.getTimeId()).setCount(newMidInfo.getWeight());
				newUserInfoBuiler.addTsegs(0,tsBuilder.build());
								
				isAdded = true;
			}

			save(userPairKey,newUserInfoBuiler.build());	
			
			new GroupPairUpdateCallback(key,itemId,midInfoMap).excute();
			
			if(key.getGroupId() != 0){
				UpdateKey noGroupKey = new UpdateKey(key.getBid(),key.getUin(),0,key.getAdpos(),key.getItemId());
				new GroupPairUpdateCallback(noGroupKey,itemId,midInfoMap).excute();
			}
		}
		
		private void save(String userPairKey,UserPairInfo newInfo){
			Future<Result<Void>> future = null;
			synchronized(userPairCache){
				userPairCache.set(userPairKey, new SoftReference<UserPairInfo>(newInfo), cacheExpireTime);
			}
			
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
				try {
					future = clientEntry.getClient().putAsync((short)nsUserPairTableId, userPairKey.getBytes(),newInfo.toByteArray(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,userPairKey,String.valueOf(newInfo).getBytes(),putopt));
					
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
			UserPairInfo oldInfo = null;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult()!=null){
					oldInfo = UserPairInfo.parseFrom(res.getResult());
				}	
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
			next(oldInfo);
		}
		
	}
	
	private class ActionDetailCallBack implements MutiClientCallBack{
		private final UpdateKey key;
		private final String checkKey;
		private final GroupActionCombinerValue values;

		public ActionDetailCallBack(UpdateKey key, GroupActionCombinerValue values){
			this.key = key ; 
			this.values = values;		
			this.checkKey =  key.getDetailKey();
		}

		private void next(String item, HashMap<String,MidInfo> weightMap){
			if(weightMap != null){
				for(String itemId:weightMap.keySet()){
					if(!itemId.equals(key.getItemId())){
						new UserPairUpdateCallBack(key, itemId, weightMap.get(itemId)).excute();
						if(debug && key.getUin() == 389687043L){
							logger.info("step1,emit to 2,item="+key.getItemId()+",qq="+key.getUin() +",otherItem="+itemId+",weight="+weightMap.get(itemId).getWeight());
						}
					}
				}
			}else{
				if(debug && key.getUin() == 389687043L){
					logger.info("step1,not found this key ,item="+key.getItemId()+",qq="+key.getUin());
				}
			}
		}
		
		public void excute() {
			try {
				ClientAttr clientEntry = mtClientList.get(0);		
				TairOption opt = new TairOption(clientEntry.getTimeout());
				Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsDetailTableId,checkKey.getBytes(),opt);
				clientEntry.getClient().notifyFuture(future, this,clientEntry);	
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}

		@Override
		public void handle(Future<?> future, Object context) {			
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult()!=null){
					UserActiveDetail oldValueHeap = UserActiveDetail.parseFrom(res.getResult());
					HashMap<String,MidInfo> weightMap = getPairItems(oldValueHeap , values);
					next(key.getItemId(),weightMap);
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}	
		}
		
		private HashMap<String,MidInfo> getPairItems(UserActiveDetail oldValueHeap, GroupActionCombinerValue values ){
			HashMap<String,MidInfo> weightMap = new HashMap<String,MidInfo>();
			
			for(TimeSegment ts:oldValueHeap.getTsegsList()){
				if(ts.getTimeId() < Utils.getDateByTime(values.getTime() - dataExpireTime)){
					continue;
				}
			
				for(ItemInfo item:ts.getItemsList()){						
					for(ActType act: item.getActsList()){	
							Float actWeight = getWeightByType(act.getActType());
							if(weightMap.containsKey(item.getItem())){
								
								if(weightMap.get(item.getItem()).getWeight() < actWeight){
									MidInfo midInfo = new MidInfo(ts.getTimeId(),actWeight);
									weightMap.put(item.getItem(), midInfo);
								}
								
							}else{
								MidInfo midInfo = new MidInfo(ts.getTimeId(),actWeight);
								weightMap.put(item.getItem(), midInfo);
							}
						
					}			
				}
			}
			
			if(weightMap.containsKey(key.getItemId())){
				MidInfo valueInfo = weightMap.remove(key.getItemId());
				for(String itemId: weightMap.keySet()){
					if(!itemId.equals(key.getItemId())){
						Float minWeight =  Math.min(weightMap.get(itemId).getWeight(), valueInfo.getWeight());
						MidInfo minWeightInfo = new MidInfo(weightMap.get(itemId).getTimeId(),minWeight);
						weightMap.put(itemId, minWeightInfo);
					}
				}
				return weightMap;
			}else{
				return null;
			}
			
		}
		
	}
}