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
import com.tencent.urs.conf.AlgModuleConf.AlgModuleInfo;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.LRUCache;
import com.tencent.urs.utils.Utils;

public class CtrActionHandler implements AlgAdpter{
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private DataCache<Recommend.UserActiveHistory> cacheMap;
	private UpdateCallBack putCallBack;
	private ConcurrentHashMap<String, ActionCombinerValue> combinerMap;
	private int nsTableID;
	
	private static Logger logger = LoggerFactory
			.getLogger(CtrActionHandler.class);
	
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
		//combinerMap.(key,value);
	}	

	@SuppressWarnings("rawtypes")
	public CtrActionHandler(Map conf){
		this.nsTableID = Utils.getInt(conf, "tableid", 11);
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.cacheMap = new DataCache(conf);
		this.combinerMap = new ConcurrentHashMap<String,ActionCombinerValue>(1024);
				
		
		this.putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_interfaceID, this.getClass().getName());
		
		int expireTime = Utils.getInt(conf, "expireTime",5*3600);
		setCombinerTime(expireTime, this);

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
		
		private SoftReference<UserActiveHistory> mergeToHeap(ActionCombinerValue newVal,SoftReference<UserActiveHistory> oldVal){
			return oldVal;
		}

		private void Save(String key,SoftReference<UserActiveHistory> value){	
			int cahceExpireTime = 5;
			cacheMap.set(key, value, cahceExpireTime);
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
	public void deal(AlgModuleInfo algInfo,Tuple input) {
		// TODO Auto-generated method stub
		//只处理浏览数据
		
		String groupId = input.getStringByField("group_id");
		String tabId = input.getStringByField("tabId");
		String itemId = input.getStringByField("itemId");
		String result = input.getStringByField("result");
		String act = input.getStringByField("action_type");
		Utils.actionType action_type = Utils.actionType.PageView;

		
		if(action_type.equals(Utils.actionType.Impress)
				||action_type.equals(Utils.actionType.Click)){
			String[] items = result.split(",",-1);
			for(String eachItem: items){
				if(Utils.isItemIdValid(eachItem)){
					String key = tabId+"#"+itemId+"#"+eachItem;
					ActionCombinerValue value = new ActionCombinerValue();
					value.init(Utils.actionType.Impress);
					combinerKeys(key,value);
				}
			}	
		}
	}
	
	public static void main(String[] args){
	}
}