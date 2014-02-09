package com.tencent.urs.bolts;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.tencent.monitor.MonitorTools;
import com.tencent.streaming.commons.bolts.config.AbstractConfigUpdateBolt;
import com.tencent.streaming.commons.spouts.tdbank.Output;
import com.tencent.tde.client.Result;
import com.tencent.tde.client.Result.ResultCode;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.error.TairFlowLimit;
import com.tencent.tde.client.error.TairRpcError;
import com.tencent.tde.client.error.TairTimeout;
import com.tencent.tde.client.impl.MutiThreadCallbackClient;
import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.combine.ActionCombinerValue;
import com.tencent.urs.combine.UpdateKey;
import com.tencent.urs.conf.DataFilterConf;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.ActionWeightInfo;
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

public class PretreatmentBolt extends AbstractConfigUpdateBolt {
	private static final long serialVersionUID = 1L;
	private List<ClientAttr> mtClientList;	
	private DataCache<String> qqCache;
	private DataCache<String> groupIdCache;
	private OutputCollector collector;
	
	private int cacheExpireTime;
	private int nsTableGroup;
	private int nsTableUin;
	
	private static Logger logger = LoggerFactory
			.getLogger(PretreatmentBolt.class);

	public PretreatmentBolt(String config, ImmutableList<Output> outputField){
		super(config, outputField, Constants.config_stream);
	}
	
	@Override 
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		super.prepare(conf, context, collector);
		this.updateConfig(super.config);
		
		this.collector = collector;
		this.qqCache = new DataCache<String>(conf);
		this.groupIdCache = new DataCache<String>(conf);
		this.collector = collector;

		
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		ClientAttr clientEntry = mtClientList.get(0);		
		TairOption opt = new TairOption(clientEntry.getTimeout(),(short)0, 24*3600);
		try {
			clientEntry.getClient().put((short) nsTableUin, "17139104".getBytes(), "389687043".getBytes(), opt);
			clientEntry.getClient().put((short) nsTableGroup, "389687043".getBytes(), "1,51|2,52|3,53".getBytes(), opt);
			logger.info("init tde ");
		} catch (Exception e){
			logger.error(e.toString());
		}
	} 
	
	@Override
	public void updateConfig(XMLConfiguration config) {		
		nsTableGroup = config.getInt("tdengine.table.group", 319);
		nsTableUin = config.getInt("tdengine.table.uin", 320);		
		cacheExpireTime = config.getInt("cache_expiretime",24*3600);
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {
		String topic = tuple.getStringByField("topic");	
		String qq = tuple.getStringByField("qq");
		String uid = tuple.getStringByField("uid");	
		
		Values outputValues = new Values();

		for(String field:tuple.getFields()){
			outputValues.add(tuple.getStringByField(field));
		}

		if(topic.equals(Constants.actions_stream)){
			if(!Utils.isQNumValid(qq)){
				if(!uid.equals("0") && uid.matches("[0-9]+")){
					new GetQQUpdateCallBack(uid,topic,outputValues).excute();
				}else{
					return;
				}
			}else{
				outputValues.add(qq);
				new GetGroupIdUpdateCallBack(qq,topic,outputValues).excute();
			}						
		}else{
			return;
		}
	}
	
	public class GetQQUpdateCallBack implements MutiClientCallBack{
		private String qq;
		private String uid;
		private String outputStream;
		private Values outputValues;
		
		public GetQQUpdateCallBack(String uid, String outputStream, Values outputValues){
			this.uid = uid;
			this.outputStream = outputStream;
			this.outputValues = outputValues;
		}
		
		@Override
		public void handle(Future<?> future, Object context) {
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() != null){
					this.qq =  new String(res.getResult());
					logger.info("get qq success, uin="+qq);
					outputValues.add(qq);
					qqCache.set(uid, new SoftReference<String>(qq),cacheExpireTime);
					
					new GetGroupIdUpdateCallBack(qq,outputStream,outputValues).excute();
				}else{
					logger.error("get qq from tde failed!");
				}
			} catch (Exception e) {
				logger.error(e.toString());
			}
			
			
		}

		public void excute() {
			
			String qq = null;
			SoftReference<String> sr = qqCache.get(uid);
			if(sr != null){
				qq = sr.get();
			}
			
			if(qq != null){
				outputValues.add(qq);
				new GetGroupIdUpdateCallBack(qq, outputStream, outputValues).excute();
			}else{
				try{
					logger.info("start qq by async-get, uid="+uid);
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsTableUin,uid.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}catch(Exception e){
					logger.error(e.toString());
				}
			}

		}
	}
	
	public class GetGroupIdUpdateCallBack implements MutiClientCallBack{
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
			String groupId = "0";
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() != null){
					String tde_gid = new String(res.getResult());
					String[] grouplist = (tde_gid).split(",|\\|");
					if(grouplist.length>=2){
						groupId = grouplist[1];
						logger.info("get groupid success, uin="+groupId);
					}else{
						logger.error("gid format is bad"+tde_gid);
					}							
				}else{
					logger.error("get groupId from tde failed! error="+res.getCode());
				}
			} catch (Exception e) {
				logger.error(e.toString());
			}
			
			outputValues.add(groupId);
			//groupIdCache.set(qq.toString(), new SoftReference<String>(groupId),cacheExpireTime);
			emitData(outputStream,outputValues);
		}
		
		public void excute(){	
			String groupId = null;
			SoftReference<String> sr = groupIdCache.get(qq);
			if(sr != null){
				groupId = sr.get();
			}
			
			if(groupId != null){
				logger.info("get groupid in cache, groupid="+groupId);
				outputValues.add(groupId);
				emitData(outputStream, outputValues);
			}else{
				try{
					logger.info("start group, uin="+qq);
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsTableGroup,qq.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}catch(Exception e){
					logger.error(e.toString());
				}
			}
		}
	}
	
	private void emitData(String outputStream, Values outputValues) {
		logger.info("output="+outputValues.toString());
		this.collector.emit(outputStream,outputValues);
	}
	
}