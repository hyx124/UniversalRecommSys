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
import com.tencent.urs.combine.ActionCombinerValue;
import com.tencent.urs.combine.GroupActionCombinerValue;
import com.tencent.urs.combine.UpdateKey;
import com.tencent.urs.conf.AlgModuleConf;
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
	private DataCache<Integer> pairItemCache;
	private UpdateCallBack putCallBack;
	private ConcurrentHashMap<UpdateKey, GroupActionCombinerValue> combinerMap;
	

	private int nsTableId;
	private int nsDetailTableId;
	private int dataExpireTime;
	private int cacheExpireTime;
	
	private static Logger logger = LoggerFactory
			.getLogger(ItemPairBolt.class);
	
	public ItemPairBolt(String config, ImmutableList<Output> outputField){
		super(config, outputField, Constants.config_stream);
	}
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		super.prepare(conf, context, collector);
		this.updateConfig(super.config);

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
		nsTableId = config.getInt("storage_table",303);
		nsDetailTableId = config.getInt("dependent_table",302);
		dataExpireTime = config.getInt("data_expiretime",1*24*3600);
		cacheExpireTime = config.getInt("cache_expiretime",3600);
		//topNum = config.getInt("topNum",30);
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
		
		if(Utils.isBidValid(bid) && Utils.isQNumValid(qq) && Utils.isGroupIdVaild(groupId) && Utils.isItemIdValid(itemId)){
			GroupActionCombinerValue value = new GroupActionCombinerValue(actType,Long.valueOf(actionTime));
			UpdateKey key = new UpdateKey(bid,Long.valueOf(qq),Integer.valueOf(groupId),adpos,"");
			combinerKeys(key,value);		
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
								new ActionDetailCheckCallBack(key,expireTimeValue).excute();
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

	private class ItemPairCountUpdateCallback implements MutiClientCallBack{
		private final UpdateKey key;
		private final String item;
		private final String putKey;
		private final Long timeId;
		private final Integer changeWeight;

		public ItemPairCountUpdateCallback(UpdateKey key, String item, Integer changeWeight,Long timeId) {
			this.key = key ; 
			this.item = item;		
			this.timeId = timeId;
			this.changeWeight = changeWeight;
			this.putKey = key.getItemId()+"#"+item+"#"+key.getGroupId();
		}
		
		public void excute() {
			try {
				if(pairItemCache.hasKey(putKey)){		
					SoftReference<Integer> oldValue = pairItemCache.get(putKey);	
					SoftReference<Integer> newValueList = new SoftReference<Integer>(oldValue.get()+changeWeight);
					Save(putKey,newValueList);
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

		private void Save(String key,SoftReference<Integer> value){	
			pairItemCache.set(key, value, cacheExpireTime);
		}
		
		@Override
		public void handle(Future<?> future, Object context) {
			// TODO Auto-generated method stub
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			try {
				String oldVal = afuture.get().getResult().toString();
				SoftReference<Integer> oldValue = 
						new SoftReference<Integer>(Integer.valueOf(oldVal));
				SoftReference<Integer> newValue = new SoftReference<Integer>(oldValue.get()+changeWeight);
				Save(putKey,newValue);
			} catch (Exception e) {
				
			}
		}
		
	}
	
	private class ActionDetailCheckCallBack implements MutiClientCallBack{
		private final UpdateKey key;
		private final String checkKey;
		private final GroupActionCombinerValue values;

		public ActionDetailCheckCallBack(UpdateKey key, GroupActionCombinerValue values){
			this.key = key ; 
			this.values = values;			
			this.checkKey =  key.getBid()+"#"+key.getUin() + "#ActionDetail";
		}

		private void next(String item, HashMap<String,Integer> itemMap){
			for(String itemId:itemMap.keySet()){
				new ItemPairCountUpdateCallback(key, itemId, itemMap.get(itemId),getWinIdByTime(values.getTime())).excute();
			}
		}
		
		public void excute() {
			try {
				if(userActionCache.hasKey(checkKey)){		
					SoftReference<UserActiveDetail> oldValueHeap = userActionCache.get(checkKey);	
					
					HashMap<String,Integer> changeItemMap = getPairItems(oldValueHeap.get() , values);
					next(key.getItemId(),changeItemMap);
					
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsDetailTableId,checkKey.getBytes(),opt);
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

		@Override
		public void handle(Future<?> future, Object context) {			
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			byte[] oldVal = null;
			try {
				oldVal = afuture.get().getResult();
				UserActiveDetail oldValueHeap = UserActiveDetail.parseFrom(oldVal);
				HashMap<String,Integer> changeItemMap = getPairItems(oldValueHeap , values);
				next(key.getItemId(),changeItemMap);
			} catch (Exception e) {
				
			}
			
		}
		
		private Long getWinIdByTime(Long time){	
			String expireId = new SimpleDateFormat("yyyyMMdd").format(time*1000L);
			return Long.valueOf(expireId);
		}
		
		//会出现昨天看了，今天又看的商品没有关联度。
		private Integer[] getWeight(Recommend.UserActiveDetail oldValueHeap){						
			Integer[] weight = {0,0};//{oldmin,newmin}
			for(TimeSegment ts:oldValueHeap.getTsegsList()){
				if(ts.getTimeId() == getWinIdByTime( values.getTime() - dataExpireTime)){
					for(ItemInfo item:ts.getItemsList()){
						if(item.getItem().equals(key.getItemId())){
							weight[1] = getMinWeightFromAct(item.getActsList());
							
							for(ActType act: item.getActsList()){		
								if(act.getActType() == values.getType()
										&& act.getCount() >= 2){	
									weight[0] =  Math.min(weight[0],act.getActType().getNumber());
									weight[1] =  Math.min(weight[1],act.getActType().getNumber());
								}else if(act.getActType() == values.getType()
										&& act.getCount() <= 1){
									weight[1] =  Math.min(weight[1],act.getActType().getNumber());							
								}else if(act.getActType() != values.getType()){
									weight[0] =  Math.min(weight[0],act.getActType().getNumber());
									weight[1] =  Math.min(weight[1],act.getActType().getNumber());
								}
							}	
						}
					}
				}
			}
			return weight;
		}
	
		
		private HashMap<String,Integer> getPairItems(UserActiveDetail oldValueHeap, GroupActionCombinerValue values ){
			Integer[] item1Weight = getWeight(oldValueHeap);//old,new
			HashMap<String,Integer> changeWeightMap = new HashMap<String,Integer>();
			
			for(Recommend.UserActiveDetail.TimeSegment tsegs:oldValueHeap.getTsegsList()){
				if(tsegs.getTimeId() == getWinIdByTime(values.getTime())){
					for(Recommend.UserActiveDetail.TimeSegment.ItemInfo item: tsegs.getItemsList()){
						if(!item.getItem().equals(key.getItemId())){
							int item2Weight = getMinWeightFromAct(item.getActsList());

							int lastMin = Math.min(item1Weight[0],item2Weight);
							int nowMin = Math.min(item1Weight[1],item2Weight);
							
							if(nowMin < lastMin){
								changeWeightMap.put(item.getItem(),  nowMin - lastMin);
							}
						}
					}
				}				
			}
			return changeWeightMap;
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