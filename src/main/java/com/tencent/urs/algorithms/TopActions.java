package com.tencent.urs.algorithms;


import java.lang.ref.SoftReference;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.protobuf.InvalidProtocolBufferException;
import com.tencent.monitor.MonitorEntry;
import com.tencent.monitor.MonitorTools;

import com.tencent.tde.client.Result;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.error.TairFlowLimit;
import com.tencent.tde.client.error.TairQueueOverflow;
import com.tencent.tde.client.error.TairRpcError;
import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.algorithms.AlgAdpter;
import com.tencent.urs.asyncupdate.UpdateCallBackContext;
import com.tencent.urs.combine.ActionCombinerValue;
import com.tencent.urs.conf.AlgModuleConf.AlgModuleInfo;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.UserActiveHistory;
import com.tencent.urs.statistics.UserActionHandler.ActionDetailUpdateAysncCallback;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.LRUCache;

public class TopActions implements AlgAdpter{
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private ConcurrentHashMap<String, ActionCombinerValue> combinerMap;
	private DataCache<Recommend.UserActiveHistory> cacheMap;
	private AlgModuleInfo algInfo;
	
	private static Logger logger = LoggerFactory
			.getLogger(TopActions.class);
	
	@SuppressWarnings("rawtypes")
	public TopActions(Map conf,AlgModuleInfo algInfo){
		this.algInfo = algInfo;
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.combinerMap = new ConcurrentHashMap<String,ActionCombinerValue>(1024);
	}

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
								new TopActionsUpdateCallBack(key,expireTimeValue).excute();
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

	private class TopActionsUpdateCallBack implements MutiClientCallBack{
		private final String key;
		private final ActionCombinerValue values;

		public TopActionsUpdateCallBack(String key, ActionCombinerValue values) {
			this.key = key ; 
			this.values = values;								
		}

		public void excute() {
			try {
				if(cacheMap.hasKey(key)){		
					SoftReference<UserActiveHistory> oldValueHeap = cacheMap.get(key);	
					SoftReference<UserActiveHistory.Builder> mergeValueHeap = 
							new SoftReference<UserActiveHistory.Builder>(Recommend.UserActiveHistory.newBuilder());
					
					mergeToHeap(values,oldValueHeap.get(),mergeValueHeap);
					Save(key,mergeValueHeap);
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)algInfo.getOutputTableId(),key.getBytes(),opt);
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
		
		private void mergeToHeap(ActionCombinerValue newValList,
				UserActiveHistory oldVal,
				SoftReference<Recommend.UserActiveHistory.Builder> updatedBuilder){			
			HashSet<String> alreadyIn = new HashSet<String>();
			for(String newItemId: newValList.getActRecodeMap().keySet()){
				if(updatedBuilder.get().getActRecordsCount() >= algInfo.getTopNum()){
					break;
				}
				updatedBuilder.get().addActRecords(newValList.getActRecodeMap().get(newItemId));
				alreadyIn.add(newItemId);
			}
					
			for(Recommend.UserActiveHistory.ActiveRecord eachOldVal:oldVal.getActRecordsList()){
				if(updatedBuilder.get().getActRecordsCount() >= algInfo.getTopNum()){
					break;
				}
				
				if(alreadyIn.contains(eachOldVal.getItem())){
					continue;
				}				

				updatedBuilder.get().addActRecords(eachOldVal);
			}
			
		}

		private void Save(String key,SoftReference<UserActiveHistory> newValueHeap){	
			Future<Result<Void>> future = null;
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, algInfo.getDataExpireTime());
				try {
					future = clientEntry.getClient().putAsync((short)algInfo.getOutputTableId(), 
										key.getBytes(), newValueHeap.toString().getBytes(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,key,newValueHeap.get().toByteArray(),putopt));
					synchronized(cacheMap){
						cacheMap.set(key, newValueHeap, algInfo.getCacheExpireTime());
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
				SoftReference<UserActiveHistory> oldValueHeap = 
						new SoftReference<UserActiveHistory>(Recommend.UserActiveHistory.parseFrom(oldVal));
				SoftReference<UserActiveHistory> newValueHeap = mergeToHeap(this.values,oldValueHeap.get());
				Save(key,newValueHeap);
			} catch (Exception e) {
				
			}
			
		}
	}

	@Override
	public void deal(AlgModuleInfo algInfo,Tuple input) {
		this.algInfo = algInfo;
		String key = input.getStringByField("qq");
		byte[] newVal = null;
		new TopActionsUpdateCallBack(key, newVal).excute();	
	}
}