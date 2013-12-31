package com.tencent.urs.statistics;

import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.UserActiveHistory;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
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
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.LRUCache;
import com.tencent.urs.utils.Utils;

public class UserActionHandler implements AlgAdpter{
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private DataCache<Recommend.UserActiveHistory> cacheMap;
	private UpdateCallBack putCallBack;
	private ConcurrentHashMap<String, ActionCombinerValue> combinerMap;
	private int nsTableID;
	private int dataExpireTime;
	private int cacheExpireTime;
	private int combinerExpireTime;
	
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
		if(combinerMap.containsKey(key)){
			ActionCombinerValue oldvalue = combinerMap.get(key);
			value.incrument(oldvalue);
		}
		
		synchronized (combinerMap) {
			combinerMap.put(key, value);
		}
	}	

	@SuppressWarnings("rawtypes")
	public UserActionHandler(Map conf){
		this.nsTableID = Utils.getInt(conf, "tableid", 11);
		
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.cacheMap = new DataCache(conf);
		this.combinerMap = new ConcurrentHashMap<String,ActionCombinerValue>(1024);
		this.putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_interfaceID, this.getClass().getName());
	
		
		this.combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5);
		this.dataExpireTime = Utils.getInt(conf, "table.expireTime", 7*24*3600);
		this.cacheExpireTime = Utils.getInt(conf, "cache.expireTime", 3600);
		
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
					SoftReference<UserActiveHistory> oldValueHeap = cacheMap.get(key);	
					SoftReference<UserActiveHistory> newValueHeap = mergeToHeap(values,oldValueHeap);
					Save(key,newValueHeap);
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
		
		private SoftReference<UserActiveHistory> mergeToHeap(ActionCombinerValue newVal,SoftReference<UserActiveHistory> oldVal){
			return oldVal;
		}

		private void Save(String key,SoftReference<UserActiveHistory> value){	
			Future<Result<Void>> future = null;
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
				try {
					future = clientEntry.getClient().putAsync((short)nsTableID, key.getBytes(), value.toString().getBytes(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,key,value.toString().getBytes(),putopt));
					synchronized(cacheMap){
						cacheMap.set(key, value, cacheExpireTime);
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
				SoftReference<UserActiveHistory> newValueHeap = mergeToHeap(this.values,oldValueHeap);
				Save(key,newValueHeap);
			} catch (Exception e) {
				
			}
			
		}
	}

	@Override
	public void deal(Tuple input) {
		// TODO Auto-generated method stub
		String key = input.getStringByField("qq");
		ActionCombinerValue value = null;
		combinerKeys(key,value);	
	}
}