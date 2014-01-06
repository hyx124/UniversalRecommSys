package com.tencent.urs.process;

import java.lang.ref.SoftReference;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.monitor.MonitorEntry;
import com.tencent.monitor.MonitorTools;
import com.tencent.tde.client.Result;
import com.tencent.tde.client.Result.ResultCode;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBackContext;
import com.tencent.urs.conf.AlgModuleConf;
import com.tencent.urs.conf.AlgModuleConf.AlgModuleInfo;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.RecommendResult;
import com.tencent.urs.protobuf.Recommend.RecommendResult.Builder;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class ResultStorageBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private List<ClientAttr> mtClientList;	
	private AlgModuleConf algConf;
	private DataCache<RecommendResult> resCache;
	private OutputCollector collector;
	private MonitorTools mt;
	
	private static Logger logger = LoggerFactory
			.getLogger(ResultStorageBolt.class);
	
	public ResultStorageBolt(AlgModuleConf conf){
		this.algConf = conf;
	}
	
	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		this.resCache = new DataCache<RecommendResult>(conf);
		this.collector = collector;
		
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		

	}
	
	public class putToTDEUpdateCallBack implements MutiClientCallBack{
		private String key;
		private RecommendResult.Result value;
		private AlgModuleInfo algInfo;
		
		public putToTDEUpdateCallBack(String key, RecommendResult.Result value, AlgModuleInfo algModuleInfo){
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
	public void execute(Tuple input) {
		
		String algId = input.getStringByField("algId");
		String key = input.getStringByField("key");
		Recommend.RecommendResult.Result value = (RecommendResult.Result) input.getValueByField("value");
		new putToTDEUpdateCallBack(key,value,algConf.getAlgInfoById(algId)).excute();
		
	}

	@Override
	public void cleanup() {		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
}