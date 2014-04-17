package com.tencent.urs.bolts;

import java.lang.ref.SoftReference;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.Stream;

import com.google.common.collect.ImmutableList;
import com.tencent.streaming.commons.bolts.config.AbstractConfigUpdateBolt;
import com.tencent.streaming.commons.spouts.tdbank.Output;
import com.tencent.tde.client.Result;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.Utils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
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
	} 
	
	@Override
	public void updateConfig(XMLConfiguration config) {		
		nsTableGroup = config.getInt("group_table", 51);
		nsTableUin = config.getInt("uin_table", 53);		
		cacheExpireTime = config.getInt("cache_expiretime",24*3600);
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {		
		try{
			String bid = tuple.getStringByField("bid");	
			String topic = tuple.getStringByField("topic");	
			String qq = tuple.getStringByField("qq");
			String uid = tuple.getStringByField("uid");		
			
			if(!topic.equals(Constants.actions_stream) && !
					topic.equals(Constants.recommend_action_stream)){
				return ;
			}
			
			Values outputValues = new Values();		
			outputValues.add(bid);
			outputValues.add(topic);
			outputValues.add(uid);
			outputValues.add(tuple.getStringByField("adpos"));
			outputValues.add(tuple.getStringByField("action_type"));
			outputValues.add(tuple.getStringByField("action_time"));
			outputValues.add(tuple.getStringByField("item_id"));
			outputValues.add(tuple.getStringByField("action_result"));	
			outputValues.add(tuple.getStringByField("imei"));	
			outputValues.add(tuple.getStringByField("platform"));	
			outputValues.add(tuple.getStringByField("lbs_info"));	
			
			if(topic.equals(Constants.actions_stream) || 
					topic.equals(Constants.recommend_action_stream)){
				if(!Utils.isQNumValid(qq)){
					if(!uid.equals("0") && !uid.equals("")){
						new GetQQUpdateCallBack(uid,outputValues).excute();				
					}else{
						return;
					}
				}else{
					outputValues.add(qq);
					new GetGroupIdUpdateCallBack(qq,outputValues).excute();
				}	
			}
		}catch(Exception e){
			logger.error(e.getMessage(), e);
		}
			
	}
	
	public class GetQQUpdateCallBack implements MutiClientCallBack{
		private String uid;
		private Values outputValues;
		
		public GetQQUpdateCallBack(String uid, Values outputValues){
			this.uid = uid;
			this.outputValues = outputValues;
		}
		
		@Override
		public void handle(Future<?> future, Object context) {
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			boolean isGetQQByUid = false; 
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() != null){
					String qq =  new String(res.getResult());					
					if(Utils.isQNumValid(qq)){
						outputValues.add(qq);
						qqCache.set(uid, new SoftReference<String>(qq),cacheExpireTime);	
						new GetGroupIdUpdateCallBack(qq,outputValues).excute();
						isGetQQByUid = true;
					}
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
			
			if(!isGetQQByUid){
				outputValues.add("0");
				outputValues.add("0");
				emitData(outputValues);
				qqCache.set(uid, new SoftReference<String>("0"),cacheExpireTime);
			}
		}

		public void excute() {
			String qq = null;
			SoftReference<String> sr = qqCache.get(uid);
			if(sr != null){
				qq = sr.get();
			}
				
			if(qq != null ){
				if(Utils.isQNumValid(qq)){
					outputValues.add(qq);
					new GetGroupIdUpdateCallBack(qq,outputValues).excute();
				}else{
					outputValues.add("0");
					outputValues.add("0");
					emitData(outputValues);
				}					
			}else{
				try{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsTableUin,uid.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}catch(Exception e){
					logger.error(e.getMessage(), e);
				}
			}
		}
	}
	
	public class GetGroupIdUpdateCallBack implements MutiClientCallBack{
		private String qq;
		private Values outputValues;
		
		public GetGroupIdUpdateCallBack(String qq,Values outputValues) {
			this.qq = qq;
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
					if(grouplist.length >= 2 && Utils.isGroupIdVaild(grouplist[1])){
						groupId = grouplist[1];		
					}				
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
			outputValues.add(groupId);
			groupIdCache.set(qq.toString(), new SoftReference<String>(groupId),cacheExpireTime);
			emitData(outputValues);
			
		}
		
		public void excute(){	
			String groupId = null;
			SoftReference<String> sr = groupIdCache.get(qq);
			if(sr != null){
				groupId = sr.get();
			}
			
			if(groupId != null){
				outputValues.add(groupId);
				emitData(outputValues);
			}else{
				try{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsTableGroup,qq.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}catch(Exception e){
					logger.error(e.getMessage(), e);
				}
			}
		}
	}
	
	private void emitData(Values outputValues) {
		String  streamTopic = outputValues.get(1).toString();
		logger.info("streamTopic = "+streamTopic+",outputValues.size()="+outputValues.size());
		synchronized(collector){		
			if(streamTopic != null &&
					streamTopic.equals(Constants.actions_stream)){
				this.collector.emit(Constants.actions_stream,outputValues);
			}else if(streamTopic != null && 
					streamTopic.equals(Constants.recommend_action_stream)){
				this.collector.emit(Constants.recommend_action_stream,outputValues);
			}else {
				this.collector.emit("no-stream-name",new Values(""));
			}
		}	
	}
	
	public static void main(String[] args){
		Values outputValues = new Values();		

		outputValues.add("adpos");
		outputValues.add("action_type");
		outputValues.add("action_time");
		outputValues.add("item_id");
		outputValues.add("action_result");	
		outputValues.add("imei");	
		outputValues.add("platform");	
		outputValues.add("lbs_info");	
		
		System.out.println(outputValues.size());
		System.out.println(outputValues.get(1).toString());
		System.out.println(outputValues.size());
		System.out.println(outputValues.size());
	}
}