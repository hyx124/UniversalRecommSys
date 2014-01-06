package com.tencent.urs.statistics;

import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.ActType;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.ActType.TimeSegment;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.ActType.TimeSegment.Item;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.Builder;
import com.tencent.urs.protobuf.Recommend.UserActiveHistory;
import com.tencent.urs.protobuf.Recommend.UserActiveHistory.ActiveRecord;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;

import com.tencent.monitor.MonitorEntry;
import com.tencent.monitor.MonitorTools;

import com.tencent.tde.client.Result;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.error.TairFlowLimit;
import com.tencent.tde.client.error.TairQueueOverflow;
import com.tencent.tde.client.error.TairRpcError;
import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.algorithms.AlgAdpter;
import com.tencent.urs.asyncupdate.UpdateCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBackContext;
import com.tencent.urs.combine.ActionCombinerValue;
import com.tencent.urs.conf.AlgModuleConf.AlgModuleInfo;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.LRUCache;
import com.tencent.urs.utils.Utils;

public class UserActionHandler implements AlgAdpter{
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private DataCache<Recommend.UserActiveDetail> cacheMap;
	private UpdateCallBack putCallBack;
	private ConcurrentHashMap<String, ActionCombinerValue> combinerMap;
	private int nsTableID;
	private int combinerExpireTime;
	
	private AlgModuleInfo algInfo;
	
	private static Logger logger = LoggerFactory
			.getLogger(UserActionHandler.class);
	
	private void setCombinerTime(final int second, final AlgAdpter bolt) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					while (true) {
						Thread.sleep(second * 1000);
						Set<String> keySet = combinerMap.keySet();
						for (String key : keySet) {
							ActionCombinerValue expireTimeValue  = combinerMap.remove(key);
							try{
								new ActionDetailUpdateAysncCallback(key,expireTimeValue).excute();
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
	
	private void combinerKeys(String key,ActionCombinerValue value) {
		synchronized (combinerMap) {
			if(combinerMap.containsKey(key)){
				ActionCombinerValue oldvalue = combinerMap.get(key);
				value.incrument(oldvalue);
			}
			combinerMap.put(key, value);
		}
	}	

	@SuppressWarnings("rawtypes")
	public UserActionHandler(Map conf){
		this.nsTableID = Utils.getInt(conf, "tableid", 11);
		
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.cacheMap = new DataCache<Recommend.UserActiveDetail>(conf);
		this.combinerMap = new ConcurrentHashMap<String,ActionCombinerValue>(1024);
		this.putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_interfaceID, this.getClass().getName());
		
		this.combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5);
		setCombinerTime(combinerExpireTime, this);
	}

	private class ActionDetailUpdateAysncCallback implements MutiClientCallBack{
		private final String key;
		private final ActionCombinerValue values;

		public ActionDetailUpdateAysncCallback(String key, ActionCombinerValue values) {
			this.key = key ; 
			this.values = values;								
		}

		public void excute() {
			try {
				if(cacheMap.hasKey(key)){		
					SoftReference<UserActiveDetail> oldValueHeap = cacheMap.get(key);	
					UserActiveDetail.Builder mergeValueBuilder = UserActiveDetail.newBuilder();
					mergeToHeap(values,oldValueHeap.get(),mergeValueBuilder);
					Save(key,mergeValueBuilder);
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsTableID,key.getBytes(),opt);
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
		
		private void addItemToBuilder(String itemId,ActiveRecord activeRecord, 
				UserActiveDetail oldValList, Builder mergeValueBuilder ){
			
			boolean find_act = false;
			boolean find_time = false;			
			Long now = System.currentTimeMillis()/1000;
			
			Item.Builder newItemBuilder = Item.newBuilder();
			newItemBuilder.setItem(itemId).setCount(1).setLastUpdateTime(now);
			
			for(ActType actType: oldValList.getTypesList()){
				ActType.Builder newActBuilder = ActType.newBuilder();
				
				if(actType.getActType() == activeRecord.getActType()){
					find_act = true;
				}
				
				for(TimeSegment timeSegList: actType.getTsegsList()){
					TimeSegment.Builder newTimeBuilder = TimeSegment.newBuilder();
					if(timeSegList.getTimeSegment() == now){
						find_time = true;
						for(Item item: timeSegList.getItemsList()){
							if(item.getItem().equals(itemId) ){
								newItemBuilder.setCount(newItemBuilder.getCount()+1);
							}else{
								newTimeBuilder.addItems(item);
							}
						}
						
						newTimeBuilder.addItems(0, newItemBuilder.build());
						newActBuilder.addTsegs(0,newTimeBuilder.build());
					}else if(timeSegList.getTimeSegment() > algInfo.getDataExpireTime() && timeSegList.getTimeSegment() < now){
						newActBuilder.addTsegs(timeSegList);
					}else{
						continue;
					}					
				}
				
				
				if(!find_time){
					TimeSegment.Builder newTimeBuilder = TimeSegment.newBuilder();
					newTimeBuilder.setTimeSegment(System.currentTimeMillis()/1000);
					newTimeBuilder.addItems(0,newItemBuilder.build());
					newActBuilder.addTsegs(0,newTimeBuilder.build());
				}
				
				mergeValueBuilder.addTypes(0,newActBuilder.build());
			}
			
		
			if(! find_act){
				ActType.Builder newActBuilder = ActType.newBuilder();
				TimeSegment.Builder newTimeBuilder = TimeSegment.newBuilder();
				newTimeBuilder.addItems(0,newItemBuilder.build());
				newActBuilder.addTsegs(0,newTimeBuilder.build());
				mergeValueBuilder.addTypes(0,newActBuilder.build());
			}
			
		}
		
		private void mergeToHeap(ActionCombinerValue newValList,
				UserActiveDetail oldValList,
				UserActiveDetail.Builder mergeValueBuilder){			
		
			for(String item:newValList.getActRecodeMap().keySet()){
				addItemToBuilder(item,newValList.getActRecodeMap().get(item),oldValList,mergeValueBuilder);
			}
		}

		private void Save(String key,UserActiveDetail.Builder mergeValueBuilder){	
			Future<Result<Void>> future = null;
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, algInfo.getDataExpireTime());
				try {
					UserActiveDetail putValue = mergeValueBuilder.build();
					
					future = clientEntry.getClient().putAsync((short)algInfo.getOutputTableId(), key.getBytes(),putValue.toByteArray(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,key,putValue.toByteArray(),putopt));
					synchronized(cacheMap){
						cacheMap.set(key, new SoftReference<UserActiveDetail>(putValue), algInfo.getCacheExpireTime());
					}
					
					if(mt!=null){
						MonitorEntry mEntryPut = new MonitorEntry(Constants.SUCCESSCODE,Constants.SUCCESSCODE);
						mEntryPut.addExtField("TDW_IDC", clientEntry.getGroupname());
						mEntryPut.addExtField("tbl_name", "FIFO1");
						mt.addCountEntry(Constants.systemID, Constants.tde_put_interfaceID, mEntryPut, 1);
					}
				} catch (Exception e){
					logger.error(e.toString());
				}
			}
		}

		@Override
		public void handle(Future<?> future, Object context) {			
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			byte[] oldVal = null;
			try {
				oldVal = afuture.get().getResult();
				UserActiveDetail oldValueHeap = Recommend.UserActiveDetail.parseFrom(oldVal);
				UserActiveDetail.Builder mergeValueBuilder = UserActiveDetail.newBuilder(); 
						
				mergeToHeap(this.values,oldValueHeap,mergeValueBuilder);
				Save(key,mergeValueBuilder);
			} catch (Exception e) {
				
			}
			
		}
	}

	private ActionCombinerValue genCombinerValue(Tuple input){
		return null;
	}
	
	@Override
	public void deal(AlgModuleInfo algInfo,Tuple input) {
		String key = input.getStringByField("qq");
		ActionCombinerValue value = genCombinerValue(input);
		this.algInfo = algInfo;
		combinerKeys(key,value);	
	}


}