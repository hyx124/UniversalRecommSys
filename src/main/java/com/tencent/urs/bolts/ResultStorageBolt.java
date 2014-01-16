package com.tencent.urs.bolts;

import java.lang.ref.SoftReference;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.apache.commons.configuration.ConfigurationException;
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
import com.tencent.urs.combine.ActionCombinerValue;
import com.tencent.urs.conf.AlgModuleConf;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.RecommendResult;
import com.tencent.urs.protobuf.Recommend.RecommendResult.Builder;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.Utils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class ResultStorageBolt extends AbstractConfigUpdateBolt {
	private static final long serialVersionUID = 1L;
	private List<ClientAttr> mtClientList;	
	private AlgModuleConf algConf;
	private DataCache<RecommendResult> resCache;
	private OutputCollector collector;
	private MonitorTools mt;
	private UpdateCallBack putCallBack;
	private ConcurrentHashMap<String, RecommendResult> combinerMap;
	
	private static Logger logger = LoggerFactory
			.getLogger(ResultStorageBolt.class);

	public ResultStorageBolt(String config, ImmutableList<Output> outputField,
			String sid) {
		super(config, outputField, sid);
	}
	
	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(conf, context, collector);
		this.resCache = new DataCache<RecommendResult>(conf);
		this.collector = collector;
		
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);

		this.combinerMap = new ConcurrentHashMap<String,RecommendResult>(1024);
		this.putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_interfaceID, "ResultStorage");
		int combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5);
		//setCombinerTime(combinerExpireTime);

	}
	
	public class putToTDEUpdateCallBack implements MutiClientCallBack{
		private String key;
		private RecommendResult.Result value;
		private AlgModuleConf algInfo;
		
		public putToTDEUpdateCallBack(String key, RecommendResult.Result value, AlgModuleConf algModuleInfo){
			this.key = key;
			this.value = value;
			this.algInfo = algModuleInfo;
		}
		
		@Override
		public void handle(Future<?> future, Object context) {
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			try {
				Result<byte[]> res = afuture.get();
				if(res.getCode().equals(ResultCode.OK) && res.getResult() != null){
					RecommendResult oldValue = Recommend.RecommendResult.parseFrom(res.getResult());
					sortValues(oldValue);
				}else{
					logger.error("");
				}
			} catch (Exception e) {
				logger.error(e.toString());
			}
			
			
		}

		public void excute() {
			SoftReference<RecommendResult> oldValue = resCache.get(key);
			if(oldValue != null){
				sortValues(oldValue.get());
			}else{
				try{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)algInfo.getOutputTableId(),key.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}catch(Exception e){
					logger.error(e.toString());
				}
				//
			}

		}

		private void sortValues(RecommendResult oldValue) {
			RecommendResult.Builder mergeValueBuilder = 
					RecommendResult.newBuilder();
			
			HashSet<String> alreadyIn = new HashSet<String>();
			for(RecommendResult.Result eachItem:oldValue.getResultsList()){
				if(mergeValueBuilder.getResultsCount() > algInfo.getTopNum()){
					break;
				}
			
				
				if(!alreadyIn.contains(value.getItem()) && value.getWeight() > eachItem.getWeight()){
					mergeValueBuilder.addResults(value);
					alreadyIn.add(value.getItem());
				}
				
				if(!alreadyIn.contains(eachItem.getItem()) && eachItem.getUpdateTime() > algInfo.getDataExpireTime()){
					mergeValueBuilder.addResults(eachItem);
					alreadyIn.add(eachItem.getItem());
				}
			}
			
			
			if(!alreadyIn.contains(value.getItem())  
					&& mergeValueBuilder.getResultsCount() < algInfo.getTopNum()){
				mergeValueBuilder.addResults(value);
				alreadyIn.add(value.getItem());
			}
			
			
			SaveValues(key,mergeValueBuilder);
		}

		private void SaveValues(String key, Builder mergeValueBuilder) {
			Future<Result<Void>> future = null;
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, algInfo.getDataExpireTime());
				try {
					RecommendResult putValue = mergeValueBuilder.build();
					UpdateCallBack putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_interfaceID, algInfo.getAlgName());
					future = clientEntry.getClient().putAsync((short)algInfo.getOutputTableId(), 
										key.getBytes(), putValue.toByteArray(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,key,putValue.toByteArray(),putopt));
					synchronized(resCache){
						resCache.set(key, new SoftReference<RecommendResult>(putValue), algInfo.getCacheExpireTime());
					}
					
					if(mt!=null){
						MonitorEntry mEntryPut = new MonitorEntry(Constants.SUCCESSCODE,Constants.SUCCESSCODE);
						mEntryPut.addExtField("TDW_IDC", clientEntry.getGroupname());
						mEntryPut.addExtField("tbl_name", algInfo.getAlgName());
						mt.addCountEntry(Constants.systemID, Constants.tde_put_interfaceID, mEntryPut, 1);
					}
				} catch (Exception e){
					logger.error(e.toString());
				}
			}
			
		}
	}
	
	@Override
	public void updateConfig(XMLConfiguration config) {
		
		try {
			this.algConf.load(config);
		} catch (ConfigurationException e) {
			logger.error(e.toString());
		}	
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {		
		String algName = tuple.getStringByField("algName");
		String key = tuple.getStringByField("key");
		Recommend.RecommendResult.Result value = (RecommendResult.Result) tuple.getValueByField("value");
		new putToTDEUpdateCallBack(key,value,algConf).excute();
		
	}
	
}