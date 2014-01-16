package com.tencent.urs.bolts;

import com.google.common.collect.ImmutableList;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.ActiveType;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail;
import com.tencent.urs.protobuf.Recommend.UserActiveHistory.ActiveRecord;

import java.lang.ref.SoftReference;
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
	private ConcurrentHashMap<UpdateKey, ActionCombinerValue> actionCombinerMap;
	private int nsTableID;
	private AlgModuleConf algInfo;
	
	private static Logger logger = LoggerFactory
			.getLogger(ItemPairBolt.class);
	
	public ItemPairBolt(String config, ImmutableList<Output> outputField, String sid){
		super(config, outputField, sid);
		this.updateConfig(super.config);
	}
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		super.prepare(conf, context, collector);
		this.nsTableID = Utils.getInt(conf, "tableid", 11);
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.userActionCache = new DataCache<UserActiveDetail>(conf);
		this.actionCombinerMap = new ConcurrentHashMap<UpdateKey,ActionCombinerValue>(1024);
				
		
		this.putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_interfaceID, this.getClass().getName());
		
		int expireTime = Utils.getInt(conf, "expireTime",5*3600);
		setCombinerTime(expireTime);
	}	


	@Override
	public void updateConfig(XMLConfiguration config) {
		try {
			this.algInfo.load(config);
		} catch (ConfigurationException e) {
			logger.error(e.toString());
		}
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {
		// TODO Auto-generated method stub	
		String bid = tuple.getStringByField("bid");
		String qq = tuple.getStringByField("newqq");
		String groupId = tuple.getStringByField("group_id");
		String adpos = tuple.getStringByField("adpos");
		String itemId = tuple.getStringByField("itemId");
		
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
						Set<UpdateKey> keySet = actionCombinerMap.keySet();
						for (UpdateKey key : keySet) {
							ActionCombinerValue expireTimeValue  = actionCombinerMap.remove(key);
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
		//combinerMap.(key,value);
		if(actionCombinerMap.get(key) != null){
			
		}
		
	}	

	private class ItemPairCountUpdateCallback implements MutiClientCallBack{
		private final UpdateKey key;
		private final String item;
		private final String putKey;
		private final Integer changeWeight;

		public ItemPairCountUpdateCallback(UpdateKey key, String item, Integer changeWeight) {
			this.key = key ; 
			this.item = item;		
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
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsTableID,putKey.getBytes(),opt);
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
			int cahceExpireTime = 5;
			pairItemCache.set(key, value, cahceExpireTime);
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
		private final ActionCombinerValue values;

		public ActionDetailCheckCallBack(UpdateKey key, ActionCombinerValue values){
			this.key = key ; 
			this.values = values;			
			this.checkKey = this.key.getUin()+"#"+"AlgID";
		}

		private void next(String item, HashMap<String,Integer> itemMap){
			for(String itemId:itemMap.keySet()){
				new ItemPairCountUpdateCallback(key, itemId, itemMap.get(itemId)).excute();
			}
		}
		
		public void excute() {
			try {
				if(userActionCache.hasKey(checkKey)){		
					SoftReference<UserActiveDetail> oldValueHeap = userActionCache.get(checkKey);	
					for(String item:values.getActRecodeMap().keySet()){
						HashMap<String,Integer> changeItemMap = getPairItems(oldValueHeap.get() , values.getActRecodeMap().get(item));
						next(item,changeItemMap);
					}
					
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsTableID,checkKey.getBytes(),opt);
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
				for(String item:values.getActRecodeMap().keySet()){
					HashMap<String,Integer> changeItemMap = getPairItems(oldValueHeap , values.getActRecodeMap().get(item));
					next(item,changeItemMap);
				}
			} catch (Exception e) {
				
			}
			
		}
		
		private HashMap<String,Integer> getPairItems(UserActiveDetail oldValueHeap, ActiveRecord activeRecord){
			HashMap<String,Integer>  lastWeightMap = new HashMap<String,Integer>();		
			HashMap<String,Integer>  nowWeightMap = new HashMap<String,Integer>();	
			
			for(Recommend.UserActiveDetail.TimeSegment tsegs:oldValueHeap.getTsegsList()){
				HashMap<String,Integer>  doWeightMap = null;
				if(justExpireSoon(tsegs.getTimeId())){
					doWeightMap = lastWeightMap;
				}else if(justUpdateSoon(tsegs.getTimeId())){
					doWeightMap = nowWeightMap;
				}else{
					continue;
				}
				
				for(Recommend.UserActiveDetail.TimeSegment.ItemInfo item: tsegs.getItemsList()){
					if(item.getItem().equals(key.getItemId())){
						continue;
					}
						
					for(Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType act:item.getActsList()){
						if(nowWeightMap.containsKey(item.getItem())){
							if(act.getActType().getNumber() < nowWeightMap.get(item.getItem()) ){
								doWeightMap.put(item.getItem(), act.getActType().getNumber());
							}
						}else{
							int minWeight = Math.min(act.getActType().getNumber(), activeRecord.getActType().getNumber());
							doWeightMap.put(item.getItem(), minWeight);
						}
					}
				}
			}
						
			for(String itemId:lastWeightMap.keySet()){
				int changeWeight = 0;
				if(nowWeightMap.containsKey(itemId)){
					changeWeight = nowWeightMap.get(itemId) - lastWeightMap.get(itemId);
					
				}else{
					changeWeight = 0 - lastWeightMap.get(itemId);
				}
				
				if(changeWeight != 0){
					nowWeightMap.put(itemId, changeWeight);
				}
			}
			return nowWeightMap;
		}

		private boolean justUpdateSoon(long timeSegment) {
			// TODO Auto-generated method stub
			return false;
		}

		private boolean justExpireSoon(long timeSegment) {
			// TODO Auto-generated method stub
			return false;
		}
	}
}