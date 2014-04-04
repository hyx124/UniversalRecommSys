package com.tencent.urs.bolts;

import com.google.common.collect.ImmutableList;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.GroupPairInfo;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType;
import com.tencent.urs.protobuf.Recommend.UserPairInfo;

import java.lang.ref.SoftReference;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.tencent.monitor.MonitorTools;

import com.tencent.streaming.commons.bolts.config.AbstractConfigUpdateBolt;
import com.tencent.streaming.commons.spouts.tdbank.Output;
import com.tencent.tde.client.Result;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.impl.MutiThreadCallbackClient;
import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBackContext;
import com.tencent.urs.bolts.ItemPairStep2Bolt.MidInfo;
import com.tencent.urs.combine.GroupActionCombinerValue;
import com.tencent.urs.combine.UpdateKey;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.Utils;

public class NewItemPairStep1Bolt  extends AbstractConfigUpdateBolt{
	private static final long serialVersionUID = -3578535683081183276L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private DataCache<Recommend.UserPairInfo> userPairCache;
	private UpdateCallBack putCallBack;
	private ConcurrentHashMap<UpdateKey, GroupActionCombinerValue> combinerMap;

	private int nsDetailTableId;
	private int dataExpireTime;
	private int cacheExpireTime;
	private int nsUserPairTableId;
	private int linkedTime;
	private OutputCollector collector;

	private boolean debug;
	
	private static Logger logger = LoggerFactory
			.getLogger(NewItemPairStep1Bolt.class);
	
	public NewItemPairStep1Bolt(String config, ImmutableList<Output> outputField){
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

		this.userPairCache = new DataCache<Recommend.UserPairInfo>(conf);
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.combinerMap = new ConcurrentHashMap<UpdateKey,GroupActionCombinerValue>(1024);
		this.collector = collector;

		this.putCallBack = new UpdateCallBack(mt,this.nsUserPairTableId,debug);
		
		int combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5);
		setCombinerTime(combinerExpireTime);
	}	

	@Override
	public void updateConfig(XMLConfiguration config) {
		nsUserPairTableId = config.getInt("user_pair_table",515);
		nsDetailTableId = config.getInt("dependent_table",512);
		dataExpireTime = config.getInt("data_expiretime",7*24*3600);
		cacheExpireTime = config.getInt("cache_expiretime",3600);
		linkedTime = config.getInt("linked_time",24*3600);
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
								logger.error(e.getMessage(), e);
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
	
	private Float getWeightByType(Integer actionType){
		return Utils.getActionWeight(actionType);
	}

	private class UserPairUpdateCallBack implements MutiClientCallBack{
		
		private UpdateKey key;
		private HashMap<String,MidInfo> weightMap;
		private String userPairKey;

		public UserPairUpdateCallBack(UpdateKey key, HashMap<String,MidInfo> weightMap) {
			this.key = key ; 
			this.weightMap = weightMap;	
			this.userPairKey = key.getUserPairKey();
		}
		
		public void excute() {
			try {
				UserPairInfo oldInfo = null;
				SoftReference<UserPairInfo> sr = userPairCache.get(userPairKey);	
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
			LinkedList<UserPairInfo.ItemPairs> newUserPairInfoList = new LinkedList<UserPairInfo.ItemPairs>();	
			HashMap<String,HashMap<Long,MidInfo>> changeMap = new HashMap<String,HashMap<Long,MidInfo>>();
			
			HashSet<String> alreadyIn = new HashSet<String>();
			Long now = System.currentTimeMillis()/1000;
			if(oldWeightInfo != null){
				for(UserPairInfo.ItemPairs ipairs:oldWeightInfo.getIpairsList()){
					if(ipairs.getLastTimeId() < Utils.getDateByTime(now - linkedTime)){
						continue;
					}
					
					if(this.weightMap.containsKey(ipairs.getItemPair())){
						Long lastTimeId = ipairs.getLastTimeId();
						float lastWeight = ipairs.getLastCount();
						Long thisTimeId = weightMap.get(ipairs.getItemPair()).getTimeId();
						float thisWeight = weightMap.get(ipairs.getItemPair()).getWeight();
								
						HashMap<Long,MidInfo> midMap = new HashMap<Long,MidInfo>();
						
						if(lastTimeId ==  thisTimeId ){
							float changeWeight =  thisWeight - lastWeight;
							if(changeWeight != 0){
								MidInfo changeInfo = new MidInfo(thisTimeId, thisWeight - lastWeight);
								midMap.put(thisTimeId, changeInfo);
								changeMap.put(ipairs.getItemPair(), midMap);
							}
						}else if(lastTimeId !=  thisTimeId){
							MidInfo changeLastInfo = new MidInfo(lastTimeId, 0 - lastWeight);
							midMap.put(lastTimeId, changeLastInfo);
							
							MidInfo changeThisInfo = new MidInfo(thisTimeId, thisWeight - 0);
							midMap.put(thisTimeId, changeThisInfo);
							changeMap.put(ipairs.getItemPair(), midMap);
						}	
								
						UserPairInfo.ItemPairs.Builder ipairsBuilder = UserPairInfo.ItemPairs.newBuilder();
						ipairsBuilder.setItemPair(ipairs.getItemPair()).setLastTimeId(thisTimeId).setLastCount(thisWeight);
						newUserPairInfoList.add(ipairsBuilder.build());
						
						alreadyIn.add(ipairs.getItemPair());
					}else{
						newUserPairInfoList.add(ipairs);
					}		
				}
			}

			for(String leftKey : this.weightMap.keySet()){
				if(alreadyIn.contains(leftKey)){
					continue;
				}
			
				Long thisTimeId = weightMap.get(leftKey).getTimeId();
				float thisWeight = weightMap.get(leftKey).getWeight();
				
				MidInfo leftMidInfo = new MidInfo(thisTimeId, thisWeight);
				
				HashMap<Long,MidInfo> midMap = new HashMap<Long,MidInfo>();
				midMap.put(thisTimeId, leftMidInfo);
				changeMap.put(leftKey, midMap);
				
				UserPairInfo.ItemPairs.Builder ipairsBuilder = UserPairInfo.ItemPairs.newBuilder();
				ipairsBuilder.setItemPair(leftKey).setLastTimeId(thisTimeId).setLastCount(thisWeight);
				newUserPairInfoList.add(ipairsBuilder.build());
				
				alreadyIn.add(leftKey);
			}
			sendToNextBolt(changeMap);
			
			//-------sava in tde
			
			UserPairInfo.Builder newUserInfoBuiler = UserPairInfo.newBuilder();
			newUserInfoBuiler.addAllIpairs(newUserPairInfoList);
			save(userPairKey,newUserInfoBuiler.build());
		}
		
		private void sendToNextBolt(HashMap<String,HashMap<Long,MidInfo>> changeMap){
			for(String itemPairKey: changeMap.keySet()){
				String groupKey = Utils.spliceStringBySymbol("#", String.valueOf(key.getBid()),
						itemPairKey,String.valueOf(key.getGroupId()));
				
				String noGroupKey = Utils.spliceStringBySymbol("#", String.valueOf(key.getBid()),
						itemPairKey,"0");
				
				HashMap<Long, MidInfo> midMap = changeMap.get(itemPairKey);
					
				for(Long timeId: midMap.keySet()){
					Float weight = midMap.get(timeId).getWeight();
					if(weight != null && weight != 0){
						Values outputValues = new Values();		
						outputValues.add(groupKey);
						outputValues.add(timeId);
						outputValues.add(weight);	
						
						synchronized(collector){
							collector.emit(Constants.group_pair_stream,outputValues);
						}
						
						if(key.getGroupId() != 0){
							Values noGroupOutputValues = new Values();		
							noGroupOutputValues.add(noGroupKey);
							noGroupOutputValues.add(timeId);
							noGroupOutputValues.add(weight);	
							synchronized(collector){
								collector.emit(Constants.group_pair_stream,noGroupOutputValues);
							}
						}
					}	
				}
			}
		}
		
		private void save(String userPairKey,UserPairInfo newInfo){
			if(userPairKey.indexOf("389687043") > 0){
				logger.info("save input into tde,item_pair counts="+newInfo.getIpairsCount());
			}
			
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
				break;
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
			if(userPairKey.indexOf("389687043") > 0){
				logger.info("back from tde,and oldInfo is "+(oldInfo != null));
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
				new UserPairUpdateCallBack(key, weightMap).excute();
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
			
			Float thisItemWeight = getWeightByType(values.getType());
			if(thisItemWeight == null ){
				return null;
			}
			
			MidInfo newValueInfo = new MidInfo(Utils.getDateByTime(values.getTime()),thisItemWeight);
			String doubleKey = Utils.getItemPairKey(key.getItemId(),key.getItemId());
			weightMap.put(doubleKey,newValueInfo);
			
			for(TimeSegment ts:oldValueHeap.getTsegsList()){	
				if(ts.getTimeId() < Utils.getDateByTime(values.getTime() - linkedTime)){
					continue;
				}
				
				for(ItemInfo item:ts.getItemsList()){			
					String itemPairKey = Utils.getItemPairKey(key.getItemId(),item.getItem());
					for(ActType act: item.getActsList()){	
					    if(act.getLastUpdateTime()  > values.getTime() - linkedTime){
							Float actWeight = getWeightByType(act.getActType());
							if(weightMap.containsKey(itemPairKey)){	
								if(weightMap.get(itemPairKey).getWeight() < actWeight){
									MidInfo midInfo = new MidInfo(ts.getTimeId(),actWeight);
									weightMap.put(itemPairKey, midInfo);
								}		
							}else{
								MidInfo midInfo = new MidInfo(ts.getTimeId(),actWeight);
								weightMap.put(itemPairKey, midInfo);
							}
						}
					}			
				}
			}
			
			MidInfo doubleValue = weightMap.remove(doubleKey);
			for(String pairKey: weightMap.keySet()){
				if(!pairKey.equals(doubleKey)){
					Float minWeight =  Math.min(weightMap.get(pairKey).getWeight(), doubleValue.getWeight());
					Long minTimeId =  Math.min(weightMap.get(pairKey).getTimeId(), doubleValue.getTimeId());
					MidInfo minWeightInfo = new MidInfo(minTimeId,minWeight);
					weightMap.put(pairKey, minWeightInfo);
				}
			}
			return weightMap;
		}
		
	}
}