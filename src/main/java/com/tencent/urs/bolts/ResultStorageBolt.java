package com.tencent.urs.bolts;

import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.tencent.monitor.MonitorEntry;
import com.tencent.monitor.MonitorTools;
import com.tencent.streaming.commons.bolts.config.AbstractConfigUpdateBolt;
import com.tencent.streaming.commons.spouts.tdbank.Output;
import com.tencent.tde.client.Result;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBackContext;
import com.tencent.urs.combine.GroupActionCombinerValue;
import com.tencent.urs.combine.UpdateKey;

import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.RecommendResult;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.Utils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ResultStorageBolt extends AbstractConfigUpdateBolt {
	private static final long serialVersionUID = 1L;
	private List<ClientAttr> mtClientList;	
	private DataCache<RecommendResult> resCache;
	private MonitorTools mt;
	private int dataExpireTime;
	private int itemExpireTime;
	private int nsTableId;
	private int cacheExpireTime;
	private int topNum;
	private HashMap<String,HashMap<String,RecommendResult.Result>> liveCombinerMap;
	
	private static Logger logger = LoggerFactory
			.getLogger(ResultStorageBolt.class);

	public ResultStorageBolt(String config, ImmutableList<Output> outputField) {
		super(config, outputField, Constants.config_stream);
	}
	
	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(conf, context, collector);
		updateConfig(super.config);
		
		this.resCache = new DataCache<RecommendResult>(conf);
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		
		this.liveCombinerMap = new HashMap<String,HashMap<String,RecommendResult.Result>>(1024);
		int combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5);
		setCombinerTime(combinerExpireTime);

	}

	@Override
	public void updateConfig(XMLConfiguration config) {
		nsTableId = config.getInt("storage_table",520);
		dataExpireTime = config.getInt("data_expiretime",10*24*3600);
		itemExpireTime = config.getInt("item_expiretime",1*24*3600);
		cacheExpireTime = config.getInt("cache_expiretime",3600);
		topNum = config.getInt("top_num",100);
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {		
		//bid,key,item_id,weight,alg_name,big_type,mid_type,small_type,free,price
		try{
			//String algName = tuple.getStringByField("alg_name");
			String key = tuple.getStringByField("key");			
			String itemId = tuple.getStringByField("item_id");
			Double weight = tuple.getDoubleByField("weight");
			
			if(weight <= 0){
				return;
			}
			
			Long bigType = tuple.getLongByField("big_type");
			Long midType = tuple.getLongByField("mid_type");
			Long smallType = tuple.getLongByField("small_type");
				
			Long charType = tuple.getLongByField("small_type");
			Long price = tuple.getLongByField("price");
			String shopId = tuple.getStringByField("shop_id");
				
			RecommendResult.Result.Builder value =
					RecommendResult.Result.newBuilder();
			value.setBigType(bigType)
					.setMiddleType(midType)
					.setSmallType(smallType)
					.setPrice(price)
					.setItem(itemId).setWeight(weight).setFreeFlag(charType.intValue())
					.setUpdateTime(System.currentTimeMillis()/1000L)
					.setShopId(shopId);
			
			combinerKeys(key, value.build());	
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
						HashMap<String,HashMap<String,RecommendResult.Result>> deadCombinerMap = null;
						synchronized (liveCombinerMap) {
							deadCombinerMap = liveCombinerMap;
							liveCombinerMap = new HashMap<String,HashMap<String,RecommendResult.Result>>(1024);
						}
						
						Set<String> keySet = deadCombinerMap.keySet();
						for (String key : keySet) {
							HashMap<String, RecommendResult.Result> expireValue  = deadCombinerMap.get(key);
							try{
								new putToTDEUpdateCallBack(key,expireValue).excute();
							}catch(Exception e){
								logger.error(e.getMessage(), e);
							}
						}
						deadCombinerMap.clear();
						deadCombinerMap = null;
						
					}
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
				}
			}
		}).start();
	}
	
	private void combinerKeys(String key,RecommendResult.Result value) {
		synchronized (liveCombinerMap) {
			if(liveCombinerMap.containsKey(key)){
				HashMap<String,RecommendResult.Result> oldValue = liveCombinerMap.get(key);
				oldValue.put(value.getItem(),value);
				liveCombinerMap.put(key, oldValue);
			}else{
				HashMap<String,RecommendResult.Result> newValue =
						new HashMap<String,RecommendResult.Result>();
				newValue.put(value.getItem(), value);
				liveCombinerMap.put(key, newValue);
			}
		}
	}	
	
	public class putToTDEUpdateCallBack implements MutiClientCallBack{
		private String key;
		private HashMap<String,RecommendResult.Result> resMap;
		
		public putToTDEUpdateCallBack(String key, HashMap<String,RecommendResult.Result> value){
			this.key = key;
			this.resMap = value;
		}
		
		@Override
		public void handle(Future<?> future, Object context) {
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			RecommendResult oldValue = null;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() != null){
					oldValue = RecommendResult.parseFrom(res.getResult());
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
			sortValues(oldValue);
		}

		public void excute() {
			RecommendResult oldValue = null;
			SoftReference<RecommendResult> sr = resCache.get(key);
		    if(sr != null){
		    	oldValue = sr.get();
		    }
				
			if(oldValue != null){
				sortValues(oldValue);
			}else{
				try{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsTableId,key.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}catch(Exception e){
					logger.error(e.getMessage(), e);
				}
			}
		}

		private void sortValues(RecommendResult oldValue) {
			RecommendResult.Builder mergeValueBuilder = 
					RecommendResult.newBuilder();
			
			
			for(String itemId:resMap.keySet()){
				RecommendResult.Result eachNewValue = resMap.get(itemId);
				
				HashSet<String> alreadyIn = new HashSet<String>();
				if(oldValue != null){
					for(RecommendResult.Result eachItem:oldValue.getResultsList()){
						if(mergeValueBuilder.getResultsCount() > topNum){
							break;
						}
															
						if(!alreadyIn.contains(eachNewValue.getItem()) && eachNewValue.getWeight() >= eachItem.getWeight()){
							mergeValueBuilder.addResults(eachNewValue);
							alreadyIn.add(eachNewValue.getItem());
						}
						
						if(!alreadyIn.contains(eachItem.getItem()) 
								&& !eachItem.getItem().equals(eachNewValue.getItem())
								&& (eachItem.getUpdateTime() - eachNewValue.getUpdateTime()) < itemExpireTime
								&& eachItem.getWeight() > 0){

								mergeValueBuilder.addResults(eachItem);
								alreadyIn.add(eachItem.getItem());
						}
					}
				}
				
				if(!alreadyIn.contains(eachNewValue.getItem())  
						&& mergeValueBuilder.getResultsCount() < topNum){
					mergeValueBuilder.addResults(eachNewValue);
					alreadyIn.add(eachNewValue.getItem());
				}	
			}
			
			
			saveValues(key,mergeValueBuilder);			
		}

		private void saveValues(String key, RecommendResult.Builder mergeValueBuilder) {
			RecommendResult putValue = mergeValueBuilder.build();
			synchronized(resCache){
				resCache.set(key, new SoftReference<RecommendResult>(putValue),cacheExpireTime);
			}

			Future<Result<Void>> future = null;
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
				try {
					UpdateCallBack putCallBack = null;
					String[] keyItems = key.split("#");
					if(keyItems.length >= 5){
						putCallBack = new UpdateCallBack(mt, keyItems[3], false);
					}else{
						putCallBack = new UpdateCallBack(mt, key, false);
					}
					
					
					future = clientEntry.getClient().putAsync((short)nsTableId, 
										key.getBytes(), putValue.toByteArray(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,key,putValue.toByteArray(),putopt));

				} catch (Exception e){
					logger.error(e.getMessage(), e);
				}
			}
			
		}
	}

	
}