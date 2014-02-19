package com.tencent.urs.bolts;

import java.lang.ref.SoftReference;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import com.tencent.tde.client.Result.ResultCode;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBackContext;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.ChargeType;
import com.tencent.urs.protobuf.Recommend.RecommendResult;
import com.tencent.urs.protobuf.Recommend.UserActiveHistory;
import com.tencent.urs.protobuf.Recommend.RecommendResult.Builder;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ResultStorageBolt extends AbstractConfigUpdateBolt {
	private static final long serialVersionUID = 1L;
	private List<ClientAttr> mtClientList;	
	private DataCache<RecommendResult> resCache;
	private MonitorTools mt;
	//private ConcurrentHashMap<String, RecommendResult> combinerMap;
	private int dataExpireTime;
	private int nsTableId;
	private int cacheExpireTime;
	private int topNum;
	
	private static Logger logger = LoggerFactory
			.getLogger(ResultStorageBolt.class);

	public ResultStorageBolt(String config, ImmutableList<Output> outputField) {
		super(config, outputField, Constants.config_stream);
	}

	@Override
	public void updateConfig(XMLConfiguration config) {
		nsTableId = config.getInt("storage_table",310);
		dataExpireTime = config.getInt("data_expiretime",10*24*3600);
		cacheExpireTime = config.getInt("cache_expiretime",3600);
		topNum = config.getInt("top_num",100);
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {		
		//bid,key,item_id,weight,alg_name,big_type,mid_type,small_type,free,price
		String algName = tuple.getStringByField("alg_name");
		String key = tuple.getStringByField("key");
		String itemId = tuple.getStringByField("item_id");
		Double weight = tuple.getDoubleByField("weight");
	
		Long bigType = tuple.getLongByField("big_type");
		Long midType = tuple.getLongByField("mid_type");
		Long smallType = tuple.getLongByField("small_type");
		
		Recommend.ChargeType charType = (Recommend.ChargeType) tuple.getValueByField("free");
		Long price = tuple.getLongByField("price");
		
		logger.info("enter ,key="+key);
		Recommend.RecommendResult.Result.Builder value =
				Recommend.RecommendResult.Result.newBuilder();
		value.setBigType(bigType.intValue())
			.setMiddleType(midType.intValue())
			.setSmallType(smallType.intValue())
			.setPrice(price)
			.setItem(itemId).setWeight(weight).setFreeFlag(charType)
			.setUpdateTime(System.currentTimeMillis()/1000L);
		new putToTDEUpdateCallBack(key,value.build(),algName).excute();
		
	}
	
	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(conf, context, collector);
		updateConfig(super.config);
		
		this.resCache = new DataCache<RecommendResult>(conf);
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		
		//this.combinerMap = new ConcurrentHashMap<String,RecommendResult>(1024);
		//int combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5);
		//setCombinerTime(combinerExpireTime);

	}
	
	public class putToTDEUpdateCallBack implements MutiClientCallBack{
		private String key;
		private RecommendResult.Result value;
		private String algName;
		
		public putToTDEUpdateCallBack(String key, RecommendResult.Result value, String algName){
			this.key = key;
			this.value = value;
			this.algName = algName;
		}
		
		@Override
		public void handle(Future<?> future, Object context) {
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			RecommendResult oldValue = null;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() != null){
					oldValue = Recommend.RecommendResult.parseFrom(res.getResult());
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
			
			HashSet<String> alreadyIn = new HashSet<String>();
			if(oldValue != null){
				for(RecommendResult.Result eachItem:oldValue.getResultsList()){
					if(mergeValueBuilder.getResultsCount() > topNum){
						break;
					}
									
					if(!alreadyIn.contains(value.getItem()) && value.getWeight() >= eachItem.getWeight()){
						mergeValueBuilder.addResults(value);
						alreadyIn.add(value.getItem());
					}
					
					if(!alreadyIn.contains(eachItem.getItem()) && (eachItem.getUpdateTime() - value.getUpdateTime() ) < dataExpireTime){
						mergeValueBuilder.addResults(eachItem);
						alreadyIn.add(eachItem.getItem());
					}
				}
			}
			
			if(!alreadyIn.contains(value.getItem())  
					&& mergeValueBuilder.getResultsCount() < topNum){
				mergeValueBuilder.addResults(value);
				alreadyIn.add(value.getItem());
			}	
			logger.info("merge success ,count="+mergeValueBuilder.getResultsCount());		
			SaveValues(key,mergeValueBuilder);
		}

		private void SaveValues(String key, Builder mergeValueBuilder) {
			RecommendResult putValue = mergeValueBuilder.build();
			synchronized(resCache){
				resCache.set(key, new SoftReference<RecommendResult>(putValue),cacheExpireTime);
			}
			/*
			for(Recommend.RecommendResult.Result res:mergeValueBuilder.getResultsList()){
				logger.info(res.getItem()+",weight="+res.getWeight());
			}*/
			
			//logger.info("key ="+key+",count="+mergeValueBuilder.getResultsCount());
			Future<Result<Void>> future = null;
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
				try {
					UpdateCallBack putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_interfaceID, algName);
					future = clientEntry.getClient().putAsync((short)nsTableId, 
										key.getBytes(), putValue.toByteArray(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,key,putValue.toByteArray(),putopt));
					
					
					/*if(mt!=null){
						MonitorEntry mEntryPut = new MonitorEntry(Constants.SUCCESSCODE,Constants.SUCCESSCODE);
						mEntryPut.addExtField("TDW_IDC", clientEntry.getGroupname());
						mEntryPut.addExtField("tbl_name", algName);
						mt.addCountEntry(Constants.systemID, Constants.tde_put_interfaceID, mEntryPut, 1);
					}*/
				} catch (Exception e){
					logger.error(e.getMessage(), e);
				}
			}
			
		}
	}

	
}