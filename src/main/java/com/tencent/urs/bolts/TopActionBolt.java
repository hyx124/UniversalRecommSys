package com.tencent.urs.bolts;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.UserActiveHistory;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.Utils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

public class TopActionBolt extends AbstractConfigUpdateBolt {
	private static final long serialVersionUID = -1351459986701457961L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private HashMap<String, ActionCombinerValue> liveCombinerMap;
	private UpdateCallBack putCallBack;
	
	private int nsTableId;
	private int dataExpireTime;
	private int topNum;
	private boolean debug;

	private static Logger logger = LoggerFactory
			.getLogger(TopActionBolt.class);
	
	public TopActionBolt(String config, ImmutableList<Output> outputField) {
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
		this.putCallBack = new UpdateCallBack(mt, this.nsTableId, debug);	
		
		int combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5);
		setCombinerTime(combinerExpireTime);
	}
	
	@Override
	public void updateConfig(XMLConfiguration config) {	
		nsTableId = config.getInt("storage_table",511);
		dataExpireTime = config.getInt("data_expiretime",7*24*3600);
		topNum = config.getInt("top_num",30);
		debug = config.getBoolean("debug",false);
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {
		try{
			String bid = tuple.getStringByField("bid");
			String qq = tuple.getStringByField("qq");
			String uid = tuple.getStringByField("uid");
			String itemId = tuple.getStringByField("item_id");			
			
			String actionType = tuple.getStringByField("action_type");
			String actionTime = tuple.getStringByField("action_time");
			String lbsInfo = tuple.getStringByField("lbs_info");
			String platform = tuple.getStringByField("platform");
		
			Long bigType = tuple.getLongByField("big_type");
			Long midType = tuple.getLongByField("mid_type");
			Long smallType = tuple.getLongByField("small_type");
			Long itemTime = tuple.getLongByField("item_time");
			String shopId = tuple.getStringByField("shop_id");
			
			if(!Utils.isItemIdValid(itemId) || Utils.isRecommendAction(actionType)){
				return;
			}
			
			if(qq.equals("389687043") || qq.equals("475182144")){
				logger.info("--input to combiner---"+tuple.toString());
			}
			
			Recommend.UserActiveHistory.ActiveRecord.Builder actBuilder =
					Recommend.UserActiveHistory.ActiveRecord.newBuilder();
			actBuilder.setItem(itemId).setActTime(Long.valueOf(actionTime))
						.setActType(Integer.valueOf(actionType))
						.setBigType(bigType).setMiddleType(midType).setSmallType(smallType)
						.setLBSInfo(lbsInfo).setPlatForm(platform).setShopId(shopId)
						.setItemTime(itemTime);

			
			ActionCombinerValue value = new ActionCombinerValue();
			value.init(itemId,actBuilder.build());
			
			if(Utils.isQNumValid(qq)){
				String key = bid+"#"+qq+"#"+Constants.topN_alg_name;
				combinerKeys(key, value);
			}else if(!uid.equals("0") && !uid.equals("")){
				String key = bid+"#"+uid+"#"+Constants.topN_Noqq_alg_name;
				if(debug){
					logger.info("input ,key="+key);
				}
				combinerKeys(key, value);
			}
				
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
						HashMap<String,ActionCombinerValue> deadCombinerMap = null;
						synchronized (liveCombinerMap) {
							deadCombinerMap = liveCombinerMap;
							liveCombinerMap = new HashMap<String,ActionCombinerValue>(1024);
						}
						
						Set<String> keySet = deadCombinerMap.keySet();
						for (String key : keySet) {
							ActionCombinerValue expireValue  = deadCombinerMap.get(key);
							try{
								new TopActionsUpdateCallBack(key,expireValue).excute();
							}catch(Exception e){
								logger.error(e.getMessage(), e);
							}
						}
						deadCombinerMap.clear();
						deadCombinerMap = null;
						
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

	private class TopActionsUpdateCallBack implements MutiClientCallBack{
		private final String key;
		private final ActionCombinerValue values;

		public TopActionsUpdateCallBack(String key, ActionCombinerValue values) {
			this.key = key; 
			this.values = values;								
		}

		public void excute() {
			try{
				ClientAttr clientEntry = mtClientList.get(0);		
				TairOption opt = new TairOption(clientEntry.getTimeout());
				Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsTableId,key.getBytes(),opt);
				clientEntry.getClient().notifyFuture(future, this, clientEntry);	
			} catch (Exception e){
				logger.error(e.toString());
			}
		}
		
		private void mergeToHeap(ActionCombinerValue newValList,
				UserActiveHistory oldVal,
				UserActiveHistory.Builder updatedBuilder){	
			HashSet<String> alreadyIn = new HashSet<String>();
		
			for(String newItem: newValList.getActRecodeMap().keySet()){
				if(updatedBuilder.getActRecordsCount() >= topNum){
					break;
				}
				
				if(!alreadyIn.contains(newItem)){
					updatedBuilder.addActRecords(newValList.getActRecodeMap().get(newItem));
					alreadyIn.add(newItem);
				}
			}
			
			if(oldVal != null){
				for(Recommend.UserActiveHistory.ActiveRecord eachOldVal:oldVal.getActRecordsList()){
					if(updatedBuilder.getActRecordsCount() >= topNum){
						break;
					}			
					if(alreadyIn.contains(eachOldVal.getItem())){
						continue;
					}				
					updatedBuilder.addActRecords(eachOldVal);
				}
			}
		}

		private void Save(UserActiveHistory.Builder mergeValueBuilder){		
			UserActiveHistory putValue = mergeValueBuilder.build();
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
				try {
					Future<Result<Void>> future = 
					clientEntry.getClient().putAsync((short)nsTableId, 
										this.key.getBytes(), putValue.toByteArray(), putopt);
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
			UserActiveHistory.Builder mergeValueBuilder = Recommend.UserActiveHistory.newBuilder();
			UserActiveHistory oldValueHeap = null;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() != null)
				{
					oldValueHeap =	Recommend.UserActiveHistory.parseFrom(res.getResult());						
				}							
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
			
			mergeToHeap(this.values,oldValueHeap,mergeValueBuilder);
			Save(mergeValueBuilder);
		}
	}
	
}
