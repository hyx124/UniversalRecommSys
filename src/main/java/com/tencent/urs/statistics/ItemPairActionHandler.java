package com.tencent.urs.statistics;

import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail;
import com.tencent.urs.protobuf.Recommend.UserActiveHistory.ActiveRecord;

import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;

import com.tencent.monitor.MonitorTools;

import com.tencent.tde.client.Result;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.error.TairFlowLimit;
import com.tencent.tde.client.error.TairQueueOverflow;
import com.tencent.tde.client.error.TairRpcError;
import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.algorithms.AlgAdpter;
import com.tencent.urs.asyncupdate.UpdateCallBack;
import com.tencent.urs.combine.ActionCombinerValue;
import com.tencent.urs.combine.GroupActionCombinerValue;
import com.tencent.urs.combine.UpdateKey;
import com.tencent.urs.conf.AlgModuleConf.AlgModuleInfo;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.Utils;

public class ItemPairActionHandler implements AlgAdpter{
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private DataCache<UserActiveDetail> userActionCache;
	private DataCache<Integer> pairItemCache;
	private UpdateCallBack putCallBack;
	private ConcurrentHashMap<UpdateKey, ActionCombinerValue> actionCombinerMap;
	private int nsTableID;
	
	private static Logger logger = LoggerFactory
			.getLogger(ItemPairActionHandler.class);
	
	private void setCombinerTime(final int second, final AlgAdpter bolt) {
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
	
	private void combinerKeys(UpdateKey key,ActionCombinerValue value) {
		//combinerMap.(key,value);
		if(actionCombinerMap.get(key) != null){
			
		}
		
	}	

	@SuppressWarnings("rawtypes")
	public ItemPairActionHandler(Map conf){
		this.nsTableID = Utils.getInt(conf, "tableid", 11);
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.userActionCache = new DataCache<UserActiveDetail>(conf);
		this.actionCombinerMap = new ConcurrentHashMap<UpdateKey,ActionCombinerValue>(1024);
				
		
		this.putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_interfaceID, this.getClass().getName());
		
		int expireTime = Utils.getInt(conf, "expireTime",5*3600);
		setCombinerTime(expireTime, this);

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

	@Override
	public void deal(AlgModuleInfo algInfo,Tuple input) {
		// TODO Auto-generated method stub	
		Long uin = input.getLongByField("uin");
		Integer groupId = input.getIntegerByField("group_id");
		String adpos = input.getStringByField("adpos");
		String itemId = input.getStringByField("itemId");
		
		ActionCombinerValue value = new ActionCombinerValue();
		
		if(Utils.isQNumValid(uin) && Utils.isGroupIdVaild(groupId) && Utils.isItemIdValid(itemId)){
			UpdateKey key = new UpdateKey(uin,groupId,adpos,"");
			combinerKeys(key,value);		
		}

	}
}