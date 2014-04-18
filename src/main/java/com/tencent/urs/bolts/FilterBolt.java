package com.tencent.urs.bolts;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.tencent.monitor.MonitorTools;
import com.tencent.streaming.commons.bolts.config.AbstractConfigUpdateBolt;
import com.tencent.streaming.commons.spouts.tdbank.Output;
import com.tencent.tde.client.Result;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBackContext;

import com.tencent.urs.combine.ActionCombinerValue;
import com.tencent.urs.combine.GroupActionCombinerValue;
import com.tencent.urs.combine.UpdateKey;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType;
import com.tencent.urs.protobuf.Recommend.UserActiveHistory;
import com.tencent.urs.protobuf.Recommend.UserActiveHistory.ActiveRecord;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.Utils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

public class FilterBolt extends AbstractConfigUpdateBolt {
	private static final long serialVersionUID = -1351459986701457961L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private HashMap<String, ActionCombinerValue> liveCombinerMap;
	private UpdateCallBack putCallBack;
	
	private int nsFilterTableId;
	private int nsDetailTableId;
	private int dataExpireTime;
	private int topNum;
	private boolean debug;


	private static Logger logger = LoggerFactory
			.getLogger(FilterBolt.class);
	
	public FilterBolt(String config, ImmutableList<Output> outputField) {
		super(config, outputField,Constants.config_stream);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		super.prepare(conf, context, collector);
		updateConfig(super.config);
		
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.liveCombinerMap = new HashMap<String,ActionCombinerValue>(1024);
		this.putCallBack = new UpdateCallBack(mt, this.nsFilterTableId ,debug);	
		
		int combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5);
		setCombinerTime(combinerExpireTime);
	}
	
	@Override
	public void updateConfig(XMLConfiguration config) {	
		nsFilterTableId = config.getInt("storage_table",518);
		nsDetailTableId = config.getInt("detail_table",512);
		dataExpireTime = config.getInt("data_expiretime",180*24*3600);
		topNum = config.getInt("top_num",100);
		debug = config.getBoolean("debug",false);
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {
		try{
			String bid = tuple.getStringByField("bid");
			String qq = tuple.getStringByField("qq");
			String itemId = tuple.getStringByField("item_id");
			
			if(!Utils.isItemIdValid(itemId) || !Utils.isQNumValid(qq)){
				return;
			}
			
			String actionType = tuple.getStringByField("action_type");
			String actionTime = tuple.getStringByField("action_time");
			String lbsInfo = tuple.getStringByField("lbs_info");
			String platform = tuple.getStringByField("platform");
			
			if(Utils.isRecommendAction(actionType)){
				return;
			}
			
			Long bigType = tuple.getLongByField("big_type");
			Long midType = tuple.getLongByField("mid_type");
			Long smallType = tuple.getLongByField("small_type");

			String shopId = tuple.getStringByField("shop_id");

			if(qq.equals("389687043")){
				logger.info("input into combiner");
			}
			
			
			Recommend.UserActiveHistory.ActiveRecord.Builder actBuilder =
					Recommend.UserActiveHistory.ActiveRecord.newBuilder();
			actBuilder.setItem(itemId).setActTime(Long.valueOf(actionTime)).setActType(Integer.valueOf(actionType))
						.setBigType(bigType).setMiddleType(midType).setSmallType(smallType)
						.setLBSInfo(lbsInfo).setPlatForm(platform).setShopId(shopId);

			ActionCombinerValue value = new ActionCombinerValue();
			value.init(itemId,actBuilder.build());
			String key = Utils.spliceStringBySymbol("#", bid,qq);
			combinerKeys(key, value);	
		}catch(Exception e){
			logger.error(e.getMessage(), e);
		}
		
	}
	
	private void setCombinerTime(final int second) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					while (true) {
						Thread.sleep(second * 1000);
						HashMap<String, ActionCombinerValue> deadCombinerMap = null;
						synchronized (liveCombinerMap) {
							deadCombinerMap = liveCombinerMap;
							liveCombinerMap = new HashMap<String, ActionCombinerValue>(1024);
						}
						
						Set<String> keySet = deadCombinerMap.keySet();
						for (String key : keySet) {
							ActionCombinerValue expireValue  = deadCombinerMap.get(key);
							try{
								new CheckActionDetailCallBack(key,expireValue).excute();
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
	
	private void combinerKeys(String key,ActionCombinerValue value) {
		synchronized (liveCombinerMap) {
			if(liveCombinerMap.containsKey(key)){
				ActionCombinerValue oldvalue = liveCombinerMap.get(key);
				oldvalue.incrument(value);
				liveCombinerMap.put(key, oldvalue);
			}else{
				liveCombinerMap.put(key, value);
			}
		}
	}	
	
	private class CheckActionDetailCallBack implements MutiClientCallBack{
		private final String key;
		private ActionCombinerValue value;
		
		public CheckActionDetailCallBack(String key,ActionCombinerValue expireValue) {
			this.key = key ; 
			this.value = expireValue;
		}

		public void excute() {
			try {
				
				
				String checkKey = Utils.spliceStringBySymbol("#", key,"ActionDetail");
				
				if(key.indexOf("389687043") > 0){
					logger.info("to tde,checkKey="+checkKey);
				}
				
				ClientAttr clientEntry = mtClientList.get(0);		
				TairOption opt = new TairOption(clientEntry.getTimeout());
				Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsDetailTableId,checkKey.getBytes(),opt);
				clientEntry.getClient().notifyFuture(future, this,clientEntry);	

			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
		
		private float getFilterScoreByAct(Integer actType,Long count){
			if(actType == 1 ){
				return count*0.1F;
			}else if(actType > 1){
				return count*1.0F;
			}else{
				return 0.0F;
			}
		}
		
		private HashMap<String,Float> getFilterScore(UserActiveDetail oldValueHeap){
			if(value.getActRecodeMap().size() <= 0 ){
				return null;
			}
			
			Long now = System.currentTimeMillis()/1000;
			
			HashMap<String,Float> scoreMap = new HashMap<String,Float>();
			HashMap<String, ActiveRecord> itemMap = value.getActRecodeMap();
			for(String itemId:itemMap.keySet()){
				ActiveRecord actType = itemMap.get(itemId);
				float newFilterScore = getFilterScoreByAct(actType.getActType(), 1L);
				scoreMap.put(itemId, newFilterScore);
			}
			
			for(UserActiveDetail.TimeSegment tsegs:oldValueHeap.getTsegsList()){
				if(tsegs.getTimeId() >= Utils.getDateByTime(now - dataExpireTime)){
					for(UserActiveDetail.TimeSegment.ItemInfo item: tsegs.getItemsList()){
						for(ActType type: item.getActsList()){
							float newFilterScore = getFilterScoreByAct(type.getActType(), type.getCount());
							
							if(scoreMap.containsKey(item.getItem())){
								newFilterScore += scoreMap.get(item.getItem());
							}

							scoreMap.put(item.getItem(), newFilterScore);
						}
					}
				}
			}
			return scoreMap;
		}

		private void refreshFilterTopList(HashMap<String,Float> newFilterScoreMap,
				UserActiveHistory.Builder updatedBuilder){
			List<Map.Entry<String, Float>> sortList =
				    new ArrayList<Map.Entry<String, Float>>(newFilterScoreMap.entrySet());
			
			Collections.sort(sortList, new Comparator<Map.Entry<String, Float>>() {   
				@Override
				public int compare(Entry<String, Float> arg0,
						Entry<String, Float> arg1) {
					 return (int)(arg1.getValue() - arg0.getValue());
				}
			}); 
			
			for(Map.Entry<String, Float> sortItem: sortList){
				if(updatedBuilder.getActRecordsCount() >= topNum){
					break;
				}
				
				if(sortItem.getValue() >= 0.3){
					UserActiveHistory.ActiveRecord.Builder actBuilder =
							UserActiveHistory.ActiveRecord.newBuilder();
	
					actBuilder.setItem(sortItem.getKey()).setWeight(sortItem.getValue());
					updatedBuilder.addActRecords(actBuilder.build());
				}
			}
		}
		
		private void save(String key ,UserActiveHistory.Builder mergeValueBuilder){	
			UserActiveHistory putValue = mergeValueBuilder.build();
			
			if(key.indexOf("389687043") > 0){
				logger.info("save to tde");
			}
			
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
				try {
					Future<Result<Void>> future = 
					clientEntry.getClient().putAsync((short)nsFilterTableId, 
										key.getBytes(), putValue.toByteArray(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,key,putValue.toByteArray(),putopt));

				} catch (Exception e){
					logger.error(e.getMessage(), e);
				}
			}
		}
		
		@Override
		public void handle(Future<?> future, Object context) {			
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			try {
				Result<byte[]> result = afuture.get();	
				if(result.isSuccess() && result.getResult()!=null){
					if(key.indexOf("389687043") > 0){
						logger.info("from tde");
					}
					UserActiveDetail oldValueHeap = UserActiveDetail.parseFrom(result.getResult());
					HashMap<String,Float> newFilterScoreMap = getFilterScore(oldValueHeap);
					UserActiveHistory.Builder updatedBuilder = UserActiveHistory.newBuilder();
					if(key.indexOf("389687043") > 0){
						logger.info("newFilterScoreMap.size="+newFilterScoreMap.size());
					}
					refreshFilterTopList(newFilterScoreMap,updatedBuilder);
					
					save(key,updatedBuilder);
					
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		
		}
	
	}


}
