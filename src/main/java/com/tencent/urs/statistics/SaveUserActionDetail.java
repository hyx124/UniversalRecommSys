package com.tencent.urs.statistics;

import java.util.List;
import java.util.Map;
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
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.LRUCache;

public class SaveUserActionDetail implements AlgAdpter{
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private LRUCache<String, byte[]> combinerMap;
	private UpdateCallBack putCallBack;
	
	private static Logger logger = LoggerFactory
			.getLogger(SaveUserActionDetail.class);
	
	@SuppressWarnings("rawtypes")
	public SaveUserActionDetail(Map conf){
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.combinerMap = new LRUCache<String, byte[]>(50000);
		this.putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_interfaceID, this.getClass().getName());

	}

	private class ActionDetailUpdateAysncCallback implements MutiClientCallBack{
		private final String key;
		private final byte[] values;

		public ActionDetailUpdateAysncCallback(String key, byte[] values) {
			this.key = key ; 
			this.values = values;								
		}

		public void excute() {
			try {
				if(combinerMap.containsKey(key)){		
					byte[] oldVal = combinerMap.get(key);	
					byte[] mergeVal = mergeToHeap(values,oldVal);
					Save(key,mergeVal);
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)12,key.getBytes(),opt);
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
		
		private byte[] mergeToHeap(byte[] newVal,byte[] oldVal){
			return newVal;
		}

		private void Save(String key,byte[] values){		
			
		}

		@Override
		public void handle(Future<?> future, Object context) {			
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			byte[] oldValue = null;
			try {
				oldValue = afuture.get().getResult();
			} catch (Exception e) {
				
			}
			byte[] mergeValue = mergeToHeap(this.values,oldValue);
			Save(key,mergeValue);
		}
	}

	@Override
	public void deal(Tuple input) {
		// TODO Auto-generated method stub
		String key = input.getStringByField("qq");
		byte[] newVal = null;
		new ActionDetailUpdateAysncCallback(key, newVal).excute();		
	}
}