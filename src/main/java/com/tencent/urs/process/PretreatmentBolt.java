package com.tencent.urs.process;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.io.Input;
import com.tencent.tde.client.Result;
import com.tencent.tde.client.Result.ResultCode;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.error.TairFlowLimit;
import com.tencent.tde.client.error.TairRpcError;
import com.tencent.tde.client.error.TairTimeout;
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
	private DataCache<Long> qqCache;
	private DataCache<Integer> groupIdCache;
	private OutputCollector collector;
	
	private DataFilterConf dfConf;
	private int cacheExpireTime;
	private int nsTableGroup;
	private int nsTableUin;
	
	private static Logger logger = LoggerFactory
			.getLogger(PretreatmentBolt.class);
	
	public PretreatmentBolt(DataFilterConf dfConf){
		this.dfConf = dfConf;	
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.qqCache = new DataCache<Long>(stormConf);
		this.groupIdCache = new DataCache<Integer>(stormConf);
		this.collector = collector;
		this.cacheExpireTime = 1*24*3600;
		this.nsTableGroup = Utils.getInt(stormConf, "tdengine.table.indival", 319);
		this.nsTableUin = Utils.getInt(stormConf, "tdengine.table.indival", 320);
		
		this.mtClientList = TDEngineClientFactory.createMTClientList(stormConf);
		ClientAttr clientEntry = mtClientList.get(0);		
		TairOption opt = new TairOption(clientEntry.getTimeout(),(short)0, 24*3600);
		try {
			clientEntry.getClient().put((short) nsTableUin, "17139104".getBytes(), "389687043".getBytes(), opt);
			clientEntry.getClient().put((short) nsTableGroup, "389687043".getBytes(), "51".getBytes(), opt);
		} catch (TairRpcError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TairFlowLimit e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TairTimeout e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
					Long uin = Long.valueOf(new String(res.getResult()));
					outputValues.add(uin);
					qqCache.set(uid, new SoftReference<Long>(uin),cacheExpireTime);
					
					if(isNeedGroupId){
						new GetGroupIdUpdateCallBack(uin,outputStream,outputValues).excute();
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
			SoftReference<Long> uin = qqCache.get(uid);
			if(uin != null){
				outputValues.add(uin.get());
				if(isNeedGroupId){
					new GetGroupIdUpdateCallBack(uin.get(), outputStream, outputValues).excute();
				}else{
					emitData(outputStream, outputValues);
				}
			}else{
				try{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsTableUin,uid.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}catch(Exception e){
					logger.error(e.toString());
				}
				//
			}

		}
	}
	
	public class GetGroupIdUpdateCallBack implements MutiClientCallBack{
		private Long qq;
		private String outputStream;
		private Values outputValues;
		
		public GetGroupIdUpdateCallBack(Long qq,String outputStream, Values outputValues) {
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
					Integer groupId = Integer.valueOf(new String(res.getResult()));
					outputValues.add(groupId);
					groupIdCache.set(qq.toString(), new SoftReference<Integer>(groupId),cacheExpireTime);
					emitData(outputStream,outputValues);
				}else{
					logger.error("get groupId from tde failed!");
				}
			} catch (Exception e) {
				logger.error(e.toString());
			}
		}
		
		public void excute(){
			SoftReference<Integer> groupId = groupIdCache.get(qq.toString());
			if(groupId != null){
				outputValues.add(groupId.get());
				emitData(outputStream, outputValues);
			}else{
				try{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsTableGroup,qq.toString().getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}catch(Exception e){
					logger.error(e.toString());
				}
			}
		}
	}
	
	@Override
	public void execute(Tuple input) {
		String bid = input.getStringByField("bid");
		String topic = input.getStringByField("topic");	
		//Long qq = Long.valueOf(input.getStringByField("qq"));
		Long qq = 0L;
		String uid = input.getStringByField("uid");	
		if(!uid.equals("17139104")){
			return;
		}
		
		Values outputValues = new Values();
		String[] outputFields = dfConf.getInputFeildsByTopic(topic);
		for(String field:outputFields){
			if(!field.equals("qq")){
				String value = input.getStringByField(field);
				outputValues.add(value);
			}
		}
		
		if(dfConf.isNeedQQ(topic) ){
			if(!Utils.isQNumValid(qq)){
				if(!uid.equals("0") && uid.matches("[0-9]+")){
					new GetQQUpdateCallBack(uid,dfConf.isNeedGroupId(topic),topic,outputValues).excute();
				}else{
					return;
				}
			}else{
				outputValues.add(qq);
				if(dfConf.isNeedGroupId(topic)){
					Integer groupId = Integer.valueOf(input.getStringByField("group_id"));
					if(!Utils.isGroupIdVaild(groupId)){
						new GetGroupIdUpdateCallBack(qq,topic,outputValues);
					}else{
						outputValues.add(groupId);
						emitData(topic, outputValues);
					}					
				}else{
					emitData(topic, outputValues);
				}				
			}
		}else{
			emitData(topic, outputValues);
		}
	}
	
	private void emitData(String outputStream, Values outputValues) {
		this.collector.emit(outputStream,outputValues);
		logger.info("output="+outputValues.toString());
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub	
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		for(String topic:this.dfConf.getAllTopics()){
			ArrayList<String> outputFields = new ArrayList<String>();
			
			for(String fieldName : dfConf.getInputFeildsByTopic(topic)){
				if(!fieldName.equals("qq")){
					outputFields.add(fieldName);
				}
			}
			
			if(dfConf.isNeedQQ(topic)){
				outputFields.add("qq");
			}
			
			if(dfConf.isNeedGroupId(topic)){
				outputFields.add("group_id");
			}
			
			declarer.declareStream(topic, new Fields(outputFields));
		}	
		declarer.declareStream("filter_data", new Fields(""));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
}