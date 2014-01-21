package com.tencent.urs.bolts;

import com.google.common.collect.ImmutableList;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.CtrInfo;
import com.tencent.urs.protobuf.Recommend.CtrInfo.Builder;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType;
import com.tencent.urs.protobuf.Recommend.ActiveType;
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
import com.tencent.urs.conf.AlgModuleConf;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.Utils;

public class CtrBolt extends AbstractConfigUpdateBolt{
	private static final long serialVersionUID = 2177325954880418605L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private DataCache<Recommend.CtrInfo> ctrCache;
	private UpdateCallBack putCallBack;
	//private ConcurrentHashMap<String, ActionCombinerValue> combinerMap;
	private ConcurrentHashMap<CtrCombinerKey,Long> combinerMap;
	private AlgModuleConf algInfo;
	private int nsTableId;
	private int dataExpireTime;
	private int cacheExpireTime;
	private OutputCollector collector;
	
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

		public String getAlgKey() {
			StringBuffer getKey = new StringBuffer(bid);		
			getKey.append("#").append(pageId).append("#").append(adpos).append("#").append("CTR").append("#0");	
			return getKey.toString();
		}

		public Object getResultItem() {
			// TODO Auto-generated method stub
			return null;
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
								//mt.addCountEntry(systemID, interfaceID, item, count)
							}
						}
					}
				} catch (Exception e) {
					logger.error("Schedule thread error:" + e, e);
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
		this.ctrCache = new DataCache<Recommend.CtrInfo>(conf);
		this.combinerMap = new ConcurrentHashMap<CtrCombinerKey,Long>();
				
		
		this.putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_interfaceID, this.getClass().getName());
		
		int expireTime = Utils.getInt(conf, "expireTime",5*3600);
		setCombinerTime(expireTime);
	}
	
	@Override
	public void updateConfig(XMLConfiguration config) {
		nsTableId = config.getInt("storage_table",306);
		dataExpireTime = config.getInt("data_expiretime",1*24*3600);
		cacheExpireTime = config.getInt("cache_expiretime",3600);
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {
		String bid = tuple.getStringByField("bid");
		String qq = tuple.getStringByField("qq");
		String adpos = tuple.getStringByField("adpos");
		
		
		String actionType = tuple.getStringByField("action_type");
		String actionTime = tuple.getStringByField("action_time");
	
		ActiveType actType = Utils.getActionTypeByString(actionType);
		
		if(!Utils.isBidValid(bid) || !Utils.isQNumValid(qq)){
			return;
		}
		
		if(actType == ActiveType.Click || actType == ActiveType.Impress){
			String pageId = tuple.getStringByField("item_id");
			String actionResult = tuple.getStringByField("action_result");
			String[] items = actionResult.split(",",-1);
			
			if(Utils.isItemIdValid(pageId)){
				for(String resultItem: items){
					if(Utils.isItemIdValid(resultItem)){
						//String key =  bid+"#"+adpos+"#"+pageId+"#"+eachItem;
						CtrCombinerKey key = new CtrCombinerKey(bid,adpos,pageId,resultItem);
						combinerKeys(key,Long.valueOf(actionTime));
					}
				}
			}
		}
	}

	private class CtrUpdateCallBack implements MutiClientCallBack{
		private final CtrCombinerKey key;
		private final Long value;
		private String checkKey;

		public CtrUpdateCallBack(CtrCombinerKey key, Long value) {
			this.key = key ; 
			this.value = value;
		}
	
		public void excute() {
			try {			
					String getKey = key.getCtrCheckKey();
					if(ctrCache.hasKey(getKey)){		
						SoftReference<CtrInfo> ctr = ctrCache.get(getKey);	
						computerWeight(ctr.get());
					}else{
						ClientAttr clientEntry = mtClientList.get(0);		
						TairOption opt = new TairOption(clientEntry.getTimeout());
						Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsTableId,getKey.getBytes(),opt);
						clientEntry.getClient().notifyFuture(future, this,clientEntry);	
					}		
				
	
			} catch (TairQueueOverflow e) {
				//log.error(e.toString());
			} catch (TairRpcError e) {
				//log.error(e.toString());
			} catch (TairFlowLimit e) {
				//log.error(e.toString());
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
			byte[] oldVal = null;
			try {
				oldVal = afuture.get().getResult();
				CtrInfo oldCtr = Recommend.CtrInfo.parseFrom(oldVal);
				double weight = computerWeight(oldCtr);
				collector.emit("computer_result",new Values(key.getAlgKey(),key.getResultItem(),weight,"CTR"));
			} catch (Exception e) {
				
			}
			
		}

		private double computerWeight(CtrInfo oldCtr) {	
			Long click_sum = 0L;
			Long impress_sum = 0L;
			for(CtrInfo.TimeSegment ts:oldCtr.getTsegsList()){
				if(ts.getTimeId() <= getWinIdByTime(value)
						&& ts.getTimeId() >= getWinIdByTime(value - dataExpireTime)){
					click_sum = click_sum + ts.getClick();
					impress_sum= impress_sum + ts.getImpress();
				}else{
					continue;
				}
			}
		
			if(impress_sum >0 && click_sum > 0){
				return (double) (click_sum/impress_sum);
			}else{
				return 0;
			}	
		}
	}
	
}