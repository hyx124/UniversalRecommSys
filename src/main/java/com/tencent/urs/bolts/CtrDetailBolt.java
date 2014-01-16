package com.tencent.urs.bolts;

import com.google.common.collect.ImmutableList;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.CtrInfo;
import com.tencent.urs.protobuf.Recommend.CtrInfo.Builder;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType;
import com.tencent.urs.protobuf.Recommend.ActiveType;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail;
import com.tencent.urs.protobuf.Recommend.UserActiveHistory;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
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

import com.tencent.monitor.MonitorEntry;
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
import com.tencent.urs.combine.ActionCombinerValue;
import com.tencent.urs.conf.AlgModuleConf;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.Utils;

public class CtrDetailBolt extends AbstractConfigUpdateBolt{
	private static final long serialVersionUID = 2177325954880418605L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private DataCache<Recommend.CtrInfo> cacheMap;
	private UpdateCallBack putCallBack;
	private ConcurrentHashMap<String, ActionCombinerValue> combinerMap;
	private AlgModuleConf algInfo;
	
	private static Logger logger = LoggerFactory
			.getLogger(CtrDetailBolt.class);
	
	public CtrDetailBolt(String config, ImmutableList<Output> outputField, String sid) {
		super(config, outputField, sid);
		this.updateConfig(super.config);
	}
	
	private void setCombinerTime(final int second) {
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
								new CtrUpdateAysncCallback(key,expireTimeValue).excute();
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

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		super.prepare(conf, context, collector);
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.cacheMap = new DataCache<Recommend.CtrInfo>(conf);
		this.combinerMap = new ConcurrentHashMap<String,ActionCombinerValue>(1024);
				
		
		this.putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_interfaceID, this.getClass().getName());
		
		int expireTime = Utils.getInt(conf, "expireTime",5*3600);
		setCombinerTime(expireTime);
	}

	private class CtrUpdateAysncCallback implements MutiClientCallBack{
		//adpos#page#itemid
		private final String key;
		private final ActionCombinerValue values;

		public CtrUpdateAysncCallback(String key, ActionCombinerValue values) {
			this.key = key ; 
			this.values = values;								
		}

		public void excute() {
			try {
				if(cacheMap.hasKey(key)){		
					SoftReference<CtrInfo> oldValueHeap = cacheMap.get(key);	
					CtrInfo.Builder mergeBuilder = Recommend.CtrInfo.newBuilder();
					mergeToHeap(values,oldValueHeap.get(),mergeBuilder);
					Save(key,mergeBuilder);
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
		
		private void mergeToHeap(ActionCombinerValue newValList,
				CtrInfo oldValList,
				CtrInfo.Builder mergeValueBuilder){			
		
		}

		private void Save(String key,Builder mergeBuilder){	
			CtrInfo putValue = mergeBuilder.build();
			synchronized(cacheMap){
				cacheMap.set(key, new SoftReference<CtrInfo>(putValue), 
							algInfo.getCacheExpireTime());
			}
			
			
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, algInfo.getDataExpireTime());
				try {
										
					Future<Result<Void>> future = clientEntry.getClient().putAsync((short)algInfo.getOutputTableId(), 
									key.getBytes(),putValue.toByteArray(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,key,putValue.toByteArray(),putopt));					
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
				CtrInfo oldValueHeap = CtrInfo.parseFrom(oldVal);
				
				CtrInfo.Builder mergeBuilder = Recommend.CtrInfo.newBuilder();
				mergeToHeap(values,oldValueHeap,mergeBuilder);
				Save(key,mergeBuilder);
			} catch (Exception e) {
				
			}
			
		}
	}
	
	public static void main(String[] args){
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
		String adpos = tuple.getStringByField("adpos");
		String itemId = tuple.getStringByField("itemId");
		String result = tuple.getStringByField("result");
		
		String actionType = tuple.getStringByField("action_type");
		String actionTime = tuple.getStringByField("action_time");
	
		ActiveType actType = Utils.getActionTypeByString(actionType);
			
		if(actType == Recommend.ActiveType.Impress || actType == Recommend.ActiveType.Click){
			String[] items = result.split(",",-1);
			for(String eachItem: items){
				if(Utils.isItemIdValid(eachItem)){
					String key = adpos+"#"+itemId+"#"+eachItem;
					ActionCombinerValue value = new ActionCombinerValue();
					
					Recommend.UserActiveHistory.ActiveRecord.Builder actBuilder =
							Recommend.UserActiveHistory.ActiveRecord.newBuilder();
					actBuilder.setItem(itemId).setActTime(Long.valueOf(actionTime)).setActType(actType);
					
					value.init(itemId, actBuilder.build());
					combinerKeys(key,value);
				}
			}	
		}
	}
}