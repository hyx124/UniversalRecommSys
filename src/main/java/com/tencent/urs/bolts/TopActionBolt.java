package com.tencent.urs.bolts;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
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
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.UserActiveHistory;
import com.tencent.urs.protobuf.Recommend.UserActiveHistory.ActiveRecord;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.Utils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

public class TopActionBolt extends AbstractConfigUpdateBolt {
	private static final long serialVersionUID = -1351459986701457961L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private ConcurrentHashMap<String, ActionCombinerValue> combinerMap;
	private DataCache<Recommend.UserActiveHistory> cacheMap;
	private UpdateCallBack putCallBack;
	
	private int nsTableId;
	private int dataExpireTime;
	private int cacheExpireTime;
	private int topNum;
	private boolean debug;

	private static Logger logger = LoggerFactory
			.getLogger(TopActionBolt.class);
	
	public TopActionBolt(String config, ImmutableList<Output> outputField) {
		super(config, outputField,Constants.config_stream);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		super.prepare(conf, context, collector);
		updateConfig(super.config);
		
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.combinerMap = new ConcurrentHashMap<String,ActionCombinerValue>(1024);
		this.putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_send_interfaceID, "TopActions");	
		this.cacheMap = new DataCache<UserActiveHistory>(conf);
		
		int combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5);
		setCombinerTime(combinerExpireTime);
	}
	
	@Override
	public void updateConfig(XMLConfiguration config) {	
		nsTableId = config.getInt("storage_table",511);
		dataExpireTime = config.getInt("data_expiretime",15*24*3600);
		cacheExpireTime = config.getInt("cache_expiretime",3600);
		topNum = config.getInt("top_num",30);
		debug = config.getBoolean("debug",false);
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {
		try{
			// [1, UserAction, 389687043, 17139104, 0, 5, 1389657189, 0, , , , , 389687043, 51]
			String bid = tuple.getStringByField("bid");
			String qq = tuple.getStringByField("qq");
			String itemId = tuple.getStringByField("item_id");
			
			if(!Utils.isItemIdValid(itemId) || !Utils.isQNumValid(qq)){
				return;
			}
			
			String actionType = tuple.getStringByField("action_type");
			String actionTime = tuple.getStringByField("action_time");
			String lbsInfo = tuple.getStringByField("lbs_info");
			String platform = tuple.getStringByField("platform");
		
			Long bigType = tuple.getLongByField("big_type");
			Long midType = tuple.getLongByField("mid_type");
			Long smallType = tuple.getLongByField("small_type");

			String shopId = tuple.getStringByField("shop_id");
			
			if(Utils.isRecommendAction(actionType)){
				return;
			}
			
			Recommend.UserActiveHistory.ActiveRecord.Builder actBuilder =
					Recommend.UserActiveHistory.ActiveRecord.newBuilder();
			actBuilder.setItem(itemId).setActTime(Long.valueOf(actionTime))
						.setActType(Integer.valueOf(actionType))
						.setBigType(bigType).setMiddleType(midType).setSmallType(smallType)
						.setLBSInfo(lbsInfo).setPlatForm(platform).setShopId(shopId);

			
			ActionCombinerValue value = new ActionCombinerValue();
			value.init(itemId,actBuilder.build());
			String key = bid+"#"+qq+"#"+Constants.topN_alg_name;
			combinerKeys(key, value);	
		}catch(Exception e){
			logger.error(e.getMessage(), e);
		}
	}
	
	private void setCombinerTime(final int second) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					while (true) {
						Thread.sleep(second * 1000);
						Set<String> keySet = combinerMap.keySet();
						//logger.info("deal with="+ keySet.size()+",left size="+combinerMap.size());
						for (String key : keySet) {
							ActionCombinerValue expireValue  = combinerMap.remove(key);
							try{
								new TopActionsUpdateCallBack(key,expireValue).excute();
							}catch(Exception e){
								logger.error(e.getMessage(), e);
								//mt.addCountEntry(systemID, interfaceID, item, count)
							}
						}
						
					}
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
				}
			}
		}).start();
	}
	
	private void combinerKeys(String key,ActionCombinerValue value) {
		synchronized (combinerMap) {
			if(combinerMap.containsKey(key)){
				ActionCombinerValue oldvalue = combinerMap.get(key);
				oldvalue.incrument(value);
				combinerMap.put(key, oldvalue);
			}else{
				combinerMap.put(key, value);
			}
		}
	}	

	private class TopActionsUpdateCallBack implements MutiClientCallBack{
		private final String key;
		private final ActionCombinerValue values;

		public TopActionsUpdateCallBack(String key, ActionCombinerValue values) {
			this.key = key; 
			this.values = values;								
		}

		public void excute() {
			try{
				UserActiveHistory oldValueHeap = null;
			    SoftReference<UserActiveHistory> sr = cacheMap.get(key);
			    if(sr != null){
			    	oldValueHeap = sr.get();
			    }
			    
				if( oldValueHeap != null){
					UserActiveHistory.Builder mergeValueBuilder = Recommend.UserActiveHistory.newBuilder();
					mergeToHeap(values,oldValueHeap,mergeValueBuilder);
					Save(mergeValueBuilder);
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsTableId,key.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this, clientEntry);	
				}			
				
			} catch (Exception e){
				logger.error(e.toString());
			}
		}
		
		private void mergeToHeap(ActionCombinerValue newValList,
				UserActiveHistory oldVal,
				UserActiveHistory.Builder updatedBuilder){	
			HashSet<String> alreadyIn = new HashSet<String>();
			
			List<Map.Entry<String, UserActiveHistory.ActiveRecord>> sortList =
				    new ArrayList<Map.Entry<String, UserActiveHistory.ActiveRecord>>(newValList.getActRecodeMap().entrySet());
			
			Collections.sort(sortList, new Comparator<Map.Entry<String, UserActiveHistory.ActiveRecord>>() {   
				@Override
				public int compare(Entry<String, ActiveRecord> arg0,
						Entry<String, ActiveRecord> arg1) {
					 return (int)(arg1.getValue().getActTime() - arg0.getValue().getActTime());
				}
			}); 
			
			for(Map.Entry<String, UserActiveHistory.ActiveRecord> sortItem: sortList){
				if(updatedBuilder.getActRecordsCount() >= topNum){
					break;
				}
				
				if(!alreadyIn.contains(sortItem.getKey())){
					updatedBuilder.addActRecords(sortItem.getValue());
					alreadyIn.add(sortItem.getKey());
				}
			}
			
			if(oldVal != null){
				for(Recommend.UserActiveHistory.ActiveRecord eachOldVal:oldVal.getActRecordsList()){
					if(updatedBuilder.getActRecordsCount() >= topNum){
						break;
					}
					
					if(alreadyIn.contains(eachOldVal.getItem())){
						continue;
					}				

					updatedBuilder.addActRecords(eachOldVal);
				}
			}
		}

		private void Save(UserActiveHistory.Builder mergeValueBuilder){	
			if(debug){
				logger.info("in save,size="+mergeValueBuilder.getActRecordsCount());
				for(Recommend.UserActiveHistory.ActiveRecord act:mergeValueBuilder.getActRecordsList()){
					logger.info("new itemId = "+act.getItem()+",time="+act.getActTime()+",type="+act.getActType());
				}
			}
			
			
			UserActiveHistory putValue = mergeValueBuilder.build();
			synchronized(cacheMap){
				cacheMap.set(key, new SoftReference<UserActiveHistory>(putValue), cacheExpireTime);
			}
			
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
				try {
					Future<Result<Void>> future = 
					clientEntry.getClient().putAsync((short)nsTableId, 
										this.key.getBytes(), putValue.toByteArray(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,key,putValue.toByteArray(),putopt));

					/*
					if(mt!=null){
						MonitorEntry mEntryPut = new MonitorEntry(Constants.SUCCESSCODE,Constants.SUCCESSCODE);
						mEntryPut.addExtField("TDW_IDC", clientEntry.getGroupname());
						mEntryPut.addExtField("tbl_name", "TopActions");
						mt.addCountEntry(Constants.systemID, Constants.tde_put_interfaceID, mEntryPut, 1);
					}*/
				} catch (Exception e){
					logger.error(e.getMessage(), e);
				}
			}
		}

		@Override
		public void handle(Future<?> future, Object context) {			
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			UserActiveHistory.Builder mergeValueBuilder = Recommend.UserActiveHistory.newBuilder();
			UserActiveHistory oldValueHeap = null;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() != null)
				{
					oldValueHeap =	Recommend.UserActiveHistory.parseFrom(res.getResult());						
				}							
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
			
			mergeToHeap(this.values,oldValueHeap,mergeValueBuilder);
			Save(mergeValueBuilder);
		}
	}
	
}
