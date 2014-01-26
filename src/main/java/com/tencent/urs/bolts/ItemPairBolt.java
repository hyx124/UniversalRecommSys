package com.tencent.urs.bolts;

import com.google.common.collect.ImmutableList;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.ActiveType;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType;
import com.tencent.urs.protobuf.Recommend.UserActiveHistory.ActiveRecord;

import java.lang.ref.SoftReference;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.apache.commons.configuration.ConfigurationException;
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
	private MonitorTools mt;
	private DataCache<UserActiveDetail> userActionCache;
	private DataCache<Integer> groupPairCache;
	private DataCache<Integer> userPairCache;
	private UpdateCallBack putCallBack;
	private ConcurrentHashMap<UpdateKey, GroupActionCombinerValue> combinerMap;
	

	private int nsDetailTableId;
	private int dataExpireTime;
	private int cacheExpireTime;
	private int nsUserPairTableId;
	private int nsGroupPairTableId;
	
	private static Logger logger = LoggerFactory
			.getLogger(ItemPairBolt.class);
	
	public ItemPairBolt(String config, ImmutableList<Output> outputField){
		super(config, outputField, Constants.config_stream);
	}
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		super.prepare(conf, context, collector);
		this.updateConfig(super.config);

		this.groupPairCache = new DataCache<Integer>(conf);
		this.userPairCache = new DataCache<Integer>(conf);
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.userActionCache = new DataCache<UserActiveDetail>(conf);
		this.combinerMap = new ConcurrentHashMap<UpdateKey,GroupActionCombinerValue>(1024);
				
		
		this.putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_interfaceID, this.getClass().getName());
		
		int combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5);
		setCombinerTime(combinerExpireTime);
	}	


	@Override
	public void updateConfig(XMLConfiguration config) {
		nsUserPairTableId = config.getInt("user_count_table",303);
		nsGroupPairTableId = config.getInt("group_count_table",304);
		nsDetailTableId = config.getInt("dependent_table",302);
		dataExpireTime = config.getInt("data_expiretime",1*24*3600);
		cacheExpireTime = config.getInt("cache_expiretime",3600);
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {
		// TODO Auto-generated method stub	
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

	private class GroupPairUpdateCallback implements MutiClientCallBack{
		private final UpdateKey key;
		private final String putKey;
		private final Integer changeWeight;

		public GroupPairUpdateCallback(UpdateKey key, String otherItem, Integer changeWeight) {
			this.key = key ; 
			this.changeWeight = changeWeight;
			this.putKey = key.getGroupPairKey(otherItem);
		}
		
		public void excute() {
			try {
				if(groupPairCache.hasKey(putKey)){		
					SoftReference<Integer> oldValue = groupPairCache.get(putKey);	
					Integer newCount = oldValue.get()+changeWeight;
					Save(putKey,newCount);
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsDetailTableId,putKey.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}			
				
			} catch (TairQueueOverflow e) {
				//log.error(e.toString());
			} catch (TairRpcError e) {
				//log.error(e.toString());
			} catch (TairFlowLimit e) {
				//log.error(e.toString());
			}
		}

		private void Save(String key,Integer value){	
		
			synchronized(groupPairCache){
				groupPairCache.set(key, new SoftReference<Integer>(value), cacheExpireTime);
			}
			
			Future<Result<Void>> future = null;
			logger.info("key="+key+",value="+value);
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
				try {
					future = clientEntry.getClient().putAsync((short)nsGroupPairTableId, key.getBytes(),String.valueOf(value).getBytes(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,key,String.valueOf(value).getBytes(),putopt));
					
					/*
					if(mt!=null){
						MonitorEntry mEntryPut = new MonitorEntry(Constants.SUCCESSCODE,Constants.SUCCESSCODE);
						mEntryPut.addExtField("TDW_IDC", clientEntry.getGroupname());
						mEntryPut.addExtField("tbl_name", "FIFO1");
						mt.addCountEntry(Constants.systemID, Constants.tde_put_interfaceID, mEntryPut, 1);
					}*/
				} catch (Exception e){
					logger.error(e.toString());
				}
			}
		}
		
		@Override
		public void handle(Future<?> future, Object context) {
			// TODO Auto-generated method stub
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			try {
				String oldVal = new String(afuture.get().getResult());
				Integer oldValue = Integer.valueOf(oldVal);
				Integer newValue = oldValue + this.changeWeight;
				Save(putKey,newValue);
			} catch (Exception e) {
				Save(putKey,this.changeWeight);
			}
		}
		
	}
	
	private class UserPairUpdateCallBack implements MutiClientCallBack{
		
		private UpdateKey key;
		private Integer count;
		private String userPairKey;
		private String itemId;

		public UserPairUpdateCallBack(UpdateKey key,String otherItem, Integer count) {
			this.key = key ; 
			this.count = count;	
			this.itemId = otherItem;
			
			this.userPairKey = key.getUserPairKey(otherItem);
		}
		
		public void excute() {
			try {
				if(userPairCache.hasKey(userPairKey)){		
					SoftReference<Integer> oldCount = userPairCache.get(userPairKey);	
					next(oldCount.get());
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsUserPairTableId,userPairKey.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}			
				
			} catch (TairQueueOverflow e) {
				//log.error(e.toString());
			} catch (TairRpcError e) {
				//log.error(e.toString());
			} catch (TairFlowLimit e) {
				//log.error(e.toString());
			}
		}
		
		public void next(Integer oldCount){
			logger.info("get step2 key="+userPairKey+",old count="+oldCount+",new count="+count);
			if(oldCount == 0 && count >0){
				new GroupPairUpdateCallback(key,itemId,count).excute();
				
				UpdateKey noGroupKey = new UpdateKey(key.getBid(),key.getUin(),0,key.getAdpos(),key.getItemId());
				new GroupPairUpdateCallback(noGroupKey,itemId,count-oldCount).excute();
				
				save(userPairKey,count);
			}else if(oldCount > 0 && count < oldCount){
				new GroupPairUpdateCallback(key,itemId,count-oldCount).excute();
				
				UpdateKey noGroupKey = new UpdateKey(key.getBid(),key.getUin(),0,key.getAdpos(),key.getItemId());
				new GroupPairUpdateCallback(noGroupKey,itemId,count-oldCount).excute();
				
				save(userPairKey,count);
			}
			
		}
		
		private void save(String userPairKey,Integer count){
			Future<Result<Void>> future = null;
			synchronized(userPairCache){
				userPairCache.set(userPairKey, new SoftReference<Integer>(count), cacheExpireTime);
			}
			
			logger.info("key="+userPairKey+",value="+count);
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
				try {
					future = clientEntry.getClient().putAsync((short)nsUserPairTableId, userPairKey.getBytes(),String.valueOf(count).getBytes(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,userPairKey,String.valueOf(count).getBytes(),putopt));
					
					/*
					if(mt!=null){
						MonitorEntry mEntryPut = new MonitorEntry(Constants.SUCCESSCODE,Constants.SUCCESSCODE);
						mEntryPut.addExtField("TDW_IDC", clientEntry.getGroupname());
						mEntryPut.addExtField("tbl_name", "FIFO1");
						mt.addCountEntry(Constants.systemID, Constants.tde_put_interfaceID, mEntryPut, 1);
					}*/
				} catch (Exception e){
					logger.error(e.toString());
				}
			}
		}
			
		@Override
		public void handle(Future<?> future, Object arg1) {
			// TODO Auto-generated method stub
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			Integer oldCount = 0;
			try {
				byte[] oldVal = afuture.get().getResult();
				oldCount = Integer.valueOf(new String(oldVal));
			} catch (Exception e) {
				
			}
			next(oldCount);	
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

		private void next(String item, HashMap<String,Integer> itemMap){

			for(String itemId:itemMap.keySet()){
				new UserPairUpdateCallBack(key, itemId, itemMap.get(itemId)).excute();
			}
		}
		
		public void excute() {
			try {
				ClientAttr clientEntry = mtClientList.get(0);		
				TairOption opt = new TairOption(clientEntry.getTimeout());
				Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsDetailTableId,checkKey.getBytes(),opt);
				clientEntry.getClient().notifyFuture(future, this,clientEntry);	
			} catch (TairQueueOverflow e) {
				//log.error(e.toString());
			} catch (TairRpcError e) {
				//log.error(e.toString());
			} catch (TairFlowLimit e) {
				//log.error(e.toString());
			}
		}

		@Override
		public void handle(Future<?> future, Object context) {			
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			byte[] oldVal = null;
			try {
				oldVal = afuture.get().getResult();
				UserActiveDetail oldValueHeap = UserActiveDetail.parseFrom(oldVal);
				HashMap<String,Integer> weightMap = getPairItems(oldValueHeap , values);
				next(key.getItemId(),weightMap);
			} catch (Exception e) {
				
			}
			
		}
		
		private Long getWinIdByTime(Long time){	
			String expireId = new SimpleDateFormat("yyyyMMdd").format(time*1000L);
			return Long.valueOf(expireId);
		}
		
		private Integer getWeight(Recommend.UserActiveDetail oldValueHeap){				
			Integer newWeight = values.getType().getNumber();					
			for(TimeSegment ts:oldValueHeap.getTsegsList()){
				if(ts.getTimeId() < getWinIdByTime(values.getTime() - dataExpireTime)){
					continue;
				}
			
				for(ItemInfo item:ts.getItemsList()){						
					if(item.getItem().equals(key.getItemId())){	
						for(ActType act: item.getActsList()){	
							if(act.getLastUpdateTime() > (values.getTime() - dataExpireTime)){
								newWeight =  Math.min(newWeight,act.getActType().getNumber());
							}
						}	
					}					
				}
			}
			return newWeight;			
		}
			
		private HashMap<String,Integer> getPairItems(UserActiveDetail oldValueHeap, GroupActionCombinerValue values ){
			Integer thisWeight = getWeight(oldValueHeap);//old,new
			HashMap<String,Integer> weightMap = new HashMap<String,Integer>();
			
			for(Recommend.UserActiveDetail.TimeSegment tsegs:oldValueHeap.getTsegsList()){
				if(tsegs.getTimeId() == getWinIdByTime(values.getTime())){
					for(Recommend.UserActiveDetail.TimeSegment.ItemInfo item: tsegs.getItemsList()){
						if(!item.getItem().equals(key.getItemId())){
							int itemWeight = getMinWeightFromAct(item.getActsList());
							weightMap.put(item.getItem(),  Math.min(thisWeight, itemWeight));
						}
					}
				}				
			}
			return weightMap;
		}

		private int getMinWeightFromAct(List<ActType> actsList) {
			int min = 0;
			for(ActType act:actsList){
				if(min == 0){
					min = act.getActType().getNumber();
				}else{
					min = Math.min(act.getActType().getNumber(), min);
				}
			}
			return min;
		}
		
	}
}