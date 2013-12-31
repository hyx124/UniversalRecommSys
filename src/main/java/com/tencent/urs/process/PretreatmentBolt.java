package com.tencent.urs.process;

import java.lang.ref.SoftReference;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.io.Input;
import com.tencent.tde.client.Result;
import com.tencent.tde.client.Result.ResultCode;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.conf.AlgModuleConf;
import com.tencent.urs.conf.AlgModuleConf.AlgModuleInfo;
import com.tencent.urs.conf.DataFilterConf;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.statistics.GroupActionHandler;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.Utils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PretreatmentBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private List<ClientAttr> mtClientList;	
	private AlgModuleConf algConf;
	private DataCache<String> qqCache;
	private DataCache<String> groupIdCache;
	private OutputCollector collector;
	
	private int nsGetQQTable;
	private int nsGetGroupIdTable;
	
	private static Logger logger = LoggerFactory
			.getLogger(PretreatmentBolt.class);
	
	public PretreatmentBolt(AlgModuleConf conf){
		this.algConf = conf;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.qqCache = new DataCache(stormConf);
		this.groupIdCache = new DataCache(stormConf);
		this.collector = collector;
		
		this.mtClientList = TDEngineClientFactory.createMTClientList(stormConf);
	}
	
	public class GetQQUpdateCallBack implements MutiClientCallBack{
		private String qq;
		private String uid;
		private boolean isNeedGroupId;
		private String outputStream;
		private Values outputValues;
		
		public GetQQUpdateCallBack(String uid,boolean isNeedGroupId, String outputStream, Values outputValues){
			this.uid = uid;
			this.outputStream = outputStream;
			this.outputValues = outputValues;
			this.isNeedGroupId = isNeedGroupId;
		}
		
		@Override
		public void handle(Future<?> future, Object context) {
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			try {
				Result<byte[]> res = afuture.get();
				if(res.getCode().equals(ResultCode.OK) && res.getResult() != null){
					String uin = new String(res.getResult());
					outputValues.add(uin);
					if(isNeedGroupId){
						new GetGroupIdUpdateCallBack(qq,outputStream,outputValues).excute();
					}else{
						emitData(outputStream,outputValues);
					}
				}else{
					logger.error("get qq from tde failed!");
				}
			} catch (Exception e) {
				logger.error(e.toString());
			}
			
			
		}

		public void excute() {
			SoftReference<String> uin = qqCache.get(uid);
			if(uin != null){
				outputValues.add(uin.toString());
				if(isNeedGroupId){
					new GetGroupIdUpdateCallBack(uin.toString(), outputStream, outputValues).excute();
				}else{
					emitData(outputStream, outputValues);
				}
			}else{
				try{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsGetQQTable,uid.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}catch(Exception e){
					logger.error(e.toString());
				}
				//
			}

		}
	}
	
	public class GetGroupIdUpdateCallBack implements MutiClientCallBack{
		private String groupId;
		private String qq;
		private String outputStream;
		private Values outputValues;
		
		public GetGroupIdUpdateCallBack(String qq,String outputStream, Values outputValues) {
			this.qq = qq;
			this.outputStream = outputStream;
			this.outputValues = outputValues;
		}

		@Override
		public void handle(Future<?> future, Object context) {
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			try {
				Result<byte[]> res = afuture.get();
				if(res.getCode().equals(ResultCode.OK) && res.getResult() != null){
					String groupId = new String(res.getResult());
					outputValues.add(groupId);
					emitData(outputStream,outputValues);
				}else{
					logger.error("get groupId from tde failed!");
				}
			} catch (Exception e) {
				logger.error(e.toString());
			}
		}
		
		public void excute(){
			SoftReference<String> groupId = qqCache.get(qq);
			if(groupId != null){
				outputValues.add(qq.toString());
				emitData(outputStream, outputValues);
			}else{
				try{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsGetGroupIdTable,qq.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}catch(Exception e){
					logger.error(e.toString());
				}
			}
		}
	}
	
	@Override
	public void execute(Tuple input) {
		String topic = input.getStringByField("topic");
	
		for(AlgModuleInfo alg : algConf.getAlgList()){
			if(alg.getTopicName().equals(topic)){				
				RouteToAlgModBolt(alg,input);
			}
		}	
	}
	
	private void emitData(String outputStream, Values outputValues) {
		this.collector.emit(outputStream,outputValues);
	}

	private Values genOutputValues(AlgModuleInfo alg, Tuple input){
		Values outputValues = new Values();
		String outputFields = alg.getOutputFields();
		String[] fields = outputFields.split(",",-1);
		for(String field:fields){
			outputValues.add(input.getStringByField(field));
		}
		
		return outputValues;
	}
	
	private void RouteToAlgModBolt(AlgModuleInfo alg, Tuple input) {
		String qq = input.getStringByField("qq");
		boolean isNeedGroupId = alg.isNeedGroupId();
		String outputStream =  alg.getOutputStream();
		Values outputValues =  genOutputValues(alg,input);

		if(!Utils.isQNumValid(qq)){
			String uid = input.getStringByField("uid");
			new GetQQUpdateCallBack(uid,isNeedGroupId,outputStream,outputValues).excute();
		}else if(isNeedGroupId){
				
		}else{
			emitData(outputStream, outputValues);
		}

	}

	private String getGroupIdByUin(String uin) {
		return null;
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(Constants.user_info_stream, 
				new Fields("alg_name","bid","qq","pb_info"));
		
		declarer.declareStream(Constants.item_info_stream, 
				new Fields("alg_name","bid","item_id","pb_info"));
		
		declarer.declareStream(Constants.item_category_stream, 
				new Fields("alg_name","bid","item_id","pb_info"));
		
		declarer.declareStream(Constants.action_weight_stream, 
				new Fields("alg_name","bid","imp_date","type_id","weight"));
		
		declarer.declareStream(Constants.actions_stream, 
				new Fields("alg_name","bid","qq","group_id","item_id","lbs_info","ad_pos","action_time","action_type","action_result"));
		
		declarer.declareStream("filter_data", new Fields(""));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
}