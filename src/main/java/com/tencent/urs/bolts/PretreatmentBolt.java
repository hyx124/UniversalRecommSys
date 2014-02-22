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
import com.tencent.urs.protobuf.Recommend.ActiveType;
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
	private Recommend.ActiveType actType;
	
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
			logger.error(e.getMessage(), e);
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
		//hashkey,bid,topic,qq,uid,adpos,action_type,action_time,item_id,
		//action_result,imei,platform,lbs_info</fields>
		String bid = tuple.getStringByField("bid");	
		String topic = tuple.getStringByField("topic");	
		String qq = tuple.getStringByField("qq");
		String uid = tuple.getStringByField("uid");	
		
		String actTypeStr = tuple.getStringByField("action_type");
		this.actType = Utils.getActionTypeByString(actTypeStr);
		if(!topic.equals(Constants.actions_stream) || actType == Recommend.ActiveType.Unknown){
			return ;
		}
		
		Values outputValues = new Values();		
		outputValues.add(bid);
		outputValues.add(topic);
		outputValues.add(tuple.getStringByField("adpos"));
		outputValues.add(actTypeStr);
		outputValues.add(tuple.getStringByField("action_time"));
		outputValues.add(tuple.getStringByField("item_id"));
		outputValues.add(tuple.getStringByField("action_result"));	
		outputValues.add(tuple.getStringByField("imei"));	
		outputValues.add(tuple.getStringByField("platform"));	
		outputValues.add(tuple.getStringByField("lbs_info"));	
		
		if(!Utils.isQNumValid(qq)){
			if(!uid.equals("0") && uid.matches("[0-9]+")){
				new GetQQUpdateCallBack(uid,actType,outputValues).excute();				
			}else{
				return;
			}
		}else{
			outputValues.add(qq);
			new GetGroupIdUpdateCallBack(qq,actType,outputValues).excute();
		}						
	}
	
	public class GetQQUpdateCallBack implements MutiClientCallBack{
		private String uid;
		private Values outputValues;
		private ActiveType actType;
		
		public GetQQUpdateCallBack(String uid,ActiveType actType, Values outputValues){
			this.uid = uid;
			this.actType = actType;
			this.outputValues = outputValues;
		}
		
		@Override
		public void handle(Future<?> future, Object context) {
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() != null){
					String qq =  new String(res.getResult());					
					if(Utils.isQNumValid(qq)){
						outputValues.add(qq);
						qqCache.set(uid, new SoftReference<String>(qq),cacheExpireTime);		
						new GetGroupIdUpdateCallBack(qq,actType,outputValues).excute();
					}
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
			
			
		}

		public void excute() {
				String qq = null;
				SoftReference<String> sr = qqCache.get(uid);
				if(sr != null){
					qq = sr.get();
				}
				
				if(qq != null && Utils.isQNumValid(qq)){
					outputValues.add(qq);
					new GetGroupIdUpdateCallBack(qq,actType,outputValues).excute();
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
		private ActiveType actType;
		
		public GetGroupIdUpdateCallBack(String qq,ActiveType actType,Values outputValues) {
			this.qq = qq;
			this.outputValues = outputValues;
			this.actType = actType;
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
					if(grouplist.length >= 2){
						groupId = grouplist[1];
					}				
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
			outputValues.add(groupId);
			groupIdCache.set(qq.toString(), new SoftReference<String>(groupId),cacheExpireTime);
			emitData(actType,outputValues);
		}
		
		public void excute(){	
			String groupId = null;
			SoftReference<String> sr = groupIdCache.get(qq);
			if(sr != null){
				groupId = sr.get();
			}
			
			if(groupId != null){
				outputValues.add(groupId);
				emitData(actType,outputValues);
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
	
	private void emitData(ActiveType actType,Values outputValues) {
		synchronized(collector){
			if( actType == Recommend.ActiveType.Impress || actType == Recommend.ActiveType.Click ){
				this.collector.emit(Constants.recommend_action_stream,outputValues);
			}else{
				this.collector.emit(Constants.actions_stream,outputValues);
			}
			
		}
	}
	
}