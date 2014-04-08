package com.tencent.urs.bolts;

import com.google.common.collect.ImmutableList;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.CtrInfo;
import com.tencent.urs.protobuf.Recommend.CtrInfo.Builder;
import com.tencent.urs.protobuf.Recommend.CtrInfo.TimeSegment;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail;
import com.tencent.urs.protobuf.Recommend.UserActiveHistory;

import java.lang.ref.SoftReference;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Future;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.tencent.monitor.MonitorEntry;
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
import com.tencent.urs.bolts.CtrStorBolt.CtrCombinerValue;
import com.tencent.urs.combine.ActionCombinerValue;
import com.tencent.urs.combine.GroupActionCombinerValue;
import com.tencent.urs.combine.UpdateKey;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.Utils;

public class CtrBolt extends AbstractConfigUpdateBolt{
	private static final long serialVersionUID = 2177325954880418605L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	//private UpdateCallBack putCallBack;
	//private ConcurrentHashMap<String, ActionCombinerValue> combinerMap;
	private ConcurrentHashMap<CtrCombinerKey,Long> combinerMap;
	private int nsTableId;
	private int dataExpireTime;
	private OutputCollector collector;
	private boolean debug;
	
	private static Logger logger = LoggerFactory
			.getLogger(CtrBolt.class);
	
	public CtrBolt(String config, ImmutableList<Output> outputField) {
		super(config, outputField, Constants.config_stream);
	}
	
	public class CtrCombinerKey{
		
		private String bid;
		private String adpos;
		private String pageId;
		private String resultItem;
				
		public CtrCombinerKey(String bid,String adpos,String pageId,String resultItem){
			this.bid = bid;
			this.adpos = adpos;
			this.pageId = pageId;
			this.resultItem = resultItem;
		}

		public String getCtrCheckKey() {
			StringBuffer getKey = new StringBuffer(bid);		
			getKey.append("#").append(adpos).append("#").append(pageId).append("#").append(resultItem);	
			return getKey.toString();
		}
		
		public String getBid() {
			return bid;
		}
		
		public String getResultItem() {
			return resultItem;
		}
		
		public String getPageId() {
			return pageId;
		}
		
		public String getAdpos() {
			return adpos;
		}

	}
	
	private void setCombinerTime(final int second) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					while (true) {
						Thread.sleep(second * 1000);
						Set<CtrCombinerKey> keySet = combinerMap.keySet();
						for (CtrCombinerKey key : keySet) {
							Long value = combinerMap.remove(key);
							try{
								new CtrUpdateCallBack(key,value).excute();
							}catch(Exception e){
								logger.error(e.getMessage(), e);
							}
						}
					}
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
				}
			}
		}).start();
	}
	
	private void combinerKeys(CtrCombinerKey key,Long value) {
		combinerMap.put(key, value);
	}	

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		super.prepare(conf, context, collector);
		updateConfig(super.config);
		
		this.collector = collector;
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.combinerMap = new ConcurrentHashMap<CtrCombinerKey,Long>();
					
		int combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5)+3;
		setCombinerTime(combinerExpireTime);
	}
	
	@Override
	public void updateConfig(XMLConfiguration config) {
		nsTableId = config.getInt("storage_table",517);
		dataExpireTime = config.getInt("data_expiretime",7*24*3600);
		//cacheExpireTime = config.getInt("cache_expiretime",3600);
		debug = config.getBoolean("debug",false);
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {
		try{
			String bid = tuple.getStringByField("bid");
			String adpos = tuple.getStringByField("adpos");
			
			String actionType = tuple.getStringByField("action_type");
			String actionTime = tuple.getStringByField("action_time");

			if(Utils.isBidValid(bid) && Utils.isRecommendAction(actionType)){
				String pageId = tuple.getStringByField("item_id");
				String actionResult = tuple.getStringByField("action_result");
				String[] items = actionResult.split(";",-1);
				if(Utils.isPageIdValid(pageId)){
					for(String resultItem: items){
						if(Utils.isItemIdValid(resultItem)){
							CtrCombinerKey key = new CtrCombinerKey(bid,adpos,pageId,resultItem);
							combinerKeys(key,Long.valueOf(actionTime));
						}
					}
				}
			}
		}catch(Exception e){
			logger.error(e.getMessage(), e);
		}
	}

	private class CtrUpdateCallBack implements MutiClientCallBack{
		private final CtrCombinerKey key;
		private final Long value;

		public CtrUpdateCallBack(CtrCombinerKey key, Long value) {
			this.key = key ; 
			this.value = value;
		}
	
		public void excute() {
			try {				
				String getKey = key.getCtrCheckKey();
				ClientAttr clientEntry = mtClientList.get(0);		
				TairOption opt = new TairOption(clientEntry.getTimeout());
				Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsTableId,getKey.getBytes(),opt);
				clientEntry.getClient().notifyFuture(future, this,clientEntry);	
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
			
		
		private Long getWinIdByTime(Long time){	
			String expireId = new SimpleDateFormat("yyyyMMdd").format(time*1000L);
			return Long.valueOf(expireId);
		}
		
		@Override
		public void handle(Future<?> future, Object context) {			
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			double weight = 0;		
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() != null){
					CtrInfo oldCtr = Recommend.CtrInfo.parseFrom(res.getResult());	
					weight = computerWeight(oldCtr);
					if(weight > 0){	
						synchronized(collector){					
							String ctrKey = Utils.getAlgKey(key.getBid(), key.getPageId(), key.getAdpos(), Constants.ctr_alg_name, "0");
							collector.emit(Constants.alg_result_stream,new Values(key.getBid(),ctrKey,key.getResultItem(),weight,Constants.ctr_alg_name));
						}						
					}
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}

		private double computerWeight(CtrInfo oldCtr) {	
			Double click_sum = 0D;
			Double impress_sum = 0D;
			for(CtrInfo.TimeSegment ts:oldCtr.getTsegsList()){
				if(ts.getTimeId() >= getWinIdByTime(value - dataExpireTime)){
					click_sum = click_sum + ts.getClick();
					impress_sum = impress_sum + ts.getImpress();
				}	
			}
					
			if(debug){
				logger.info("get weight info: click ="+click_sum+",imp="+impress_sum);
			}
			
			if(impress_sum > 0 && click_sum > 0){				
				return click_sum/impress_sum;
			}else{
				return 0D;
			}	
		}
	}
	
	public static void main(String[] args){
	}
	
}