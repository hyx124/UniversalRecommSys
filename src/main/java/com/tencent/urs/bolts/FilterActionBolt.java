package com.tencent.urs.bolts;

import com.google.common.collect.ImmutableList;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.Builder;
import com.tencent.urs.protobuf.Recommend.UserActiveHistory.ActiveRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.tencent.monitor.MonitorTools;

import com.tencent.streaming.commons.bolts.config.AbstractConfigUpdateBolt;
import com.tencent.streaming.commons.spouts.tdbank.Output;
import com.tencent.tde.client.Result;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBackContext;
import com.tencent.urs.bolts.CtrBolt.CtrCombinerKey;
import com.tencent.urs.combine.ActionCombinerValue;
import com.tencent.urs.combine.UpdateKey;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.Utils;

public class FilterActionBolt extends AbstractConfigUpdateBolt{
	private static final long serialVersionUID = 5958754435166536530L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private UpdateCallBack putCallBack;
	private HashMap<String,ActionCombinerValue> liveCombinerMap;
	
	private int nsTableId;
	private int dataExpireTime;
	private int topNum;
	private boolean debug;
	
	private static Logger logger = LoggerFactory
			.getLogger(FilterActionBolt.class);
	
	public FilterActionBolt(String config, ImmutableList<Output> outputField) {
		super(config, outputField,Constants.config_stream);
	}

	@Override
	public void updateConfig(XMLConfiguration config) {
		nsTableId = config.getInt("storage_table",512);
		dataExpireTime = config.getInt("data_expiretime",7*24*3600);
		topNum = config.getInt("topNum",100);
		debug = config.getBoolean("debug",false);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector){
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
	public void processEvent(String sid, Tuple tuple) {
		try{
			String bid = tuple.getStringByField("bid");
			String adpos = Constants.DEFAULT_ADPOS;		
			String qq = tuple.getStringByField("qq");
			
			if(!Utils.isQNumValid(qq)){
				return;
			}
			
			if(qq.equals("389687043") || qq.equals("475182144")){
				logger.info("--input to combiner---"+tuple.toString());
			}
			
			String actionType = tuple.getStringByField("action_type");
			String actionTime = tuple.getStringByField("action_time");
			String lbsInfo = tuple.getStringByField("lbs_info");
			String platform = tuple.getStringByField("platform");
			
			
			if(sid.equals(Constants.recommend_action_stream) && Utils.isRecommendAction(actionType)){
				String actionResult = tuple.getStringByField("action_result");
				String[] items = actionResult.split(";",-1);
				
				for(String resultItem: items){
					if(resultItem.endsWith("#1") || resultItem.endsWith("#2")){
						resultItem = resultItem.substring(0,resultItem.length()-2);
					}
						
					if(Utils.isItemIdValid(resultItem)){
						Recommend.UserActiveHistory.ActiveRecord.Builder actBuilder =
								Recommend.UserActiveHistory.ActiveRecord.newBuilder();
						actBuilder.setItem(resultItem).setActTime(Long.valueOf(actionTime)).setActType(Integer.valueOf(actionType))
									.setLBSInfo(lbsInfo).setPlatForm(platform);
							
						ActionCombinerValue value = new ActionCombinerValue();
						value.init(resultItem,actBuilder.build());
						UpdateKey key = new UpdateKey(bid, Long.valueOf(qq), 0, adpos, resultItem);
						combinerKeys(key.getImpressDetailKey(),value);	
					}
				}
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
							ActionCombinerValue expireTimeValue  = deadCombinerMap.get(key);
							try{
								new ActionDetailUpdateAysncCallback(key,expireTimeValue).excute();
							}catch(Exception e){
								logger.error(e.getMessage(), e);
							}
						}
						deadCombinerMap.clear();
						deadCombinerMap = null;
					}
				} catch (Exception e) {
					logger.error("Schedule thread error:" + e, e);
				}
			}
		}).start();
	}
	
	private void combinerKeys(String key,ActionCombinerValue value) {
		synchronized(liveCombinerMap){
			if(liveCombinerMap.containsKey(key)){
				ActionCombinerValue oldValue = liveCombinerMap.get(key);
				oldValue.incrument(value);
				liveCombinerMap.put(key, oldValue);
			}else{
				liveCombinerMap.put(key, value);
			}
		}
	}	
	
	private class ActionDetailUpdateAysncCallback implements MutiClientCallBack{
		private final String key;
		private final ActionCombinerValue values;

		public ActionDetailUpdateAysncCallback(String key, ActionCombinerValue values) {
			this.key = key ; 
			this.values = values;								
		}

		public void excute() {
			ClientAttr clientEntry = mtClientList.get(0);		
			TairOption opt = new TairOption(clientEntry.getTimeout());
			Future<Result<byte[]>> future;
			try {
				future = clientEntry.getClient().getAsync((short)nsTableId,key.getBytes(),opt);
				clientEntry.getClient().notifyFuture(future, this,clientEntry);	
			} catch(Exception e){
				logger.error(e.getMessage(), e);
			}
		}	
		
		private void mergeOldToMap(UserActiveDetail oldValList,
				HashMap<Long,HashMap<String,HashMap<Integer,ActType>>> detailMap ){
			if(oldValList == null || oldValList.getTsegsCount() <=0 ){
				return;
			}
			
			for(UserActiveDetail.TimeSegment tsegs: oldValList.getTsegsList()){
				HashMap<String,HashMap<Integer,ActType>> itemMap;		
				if(detailMap.containsKey(tsegs.getTimeId())){
					itemMap = detailMap.get(tsegs.getTimeId());					
				}else{				
					itemMap = new HashMap<String,HashMap<Integer,ActType>>();
	
				}
				
				for(UserActiveDetail.TimeSegment.ItemInfo item: tsegs.getItemsList()){	
					HashMap<Integer,ActType> actMap;
					if(itemMap.containsKey(item.getItem())){
						actMap = itemMap.get(item.getItem());
					}else{
						actMap = new HashMap<Integer,ActType>();		
					}	
					
					for(UserActiveDetail.TimeSegment.ItemInfo.ActType act: item.getActsList()){
						Long count = act.getCount();
						if(actMap.containsKey(act.getActType())){
							count = count + actMap.get(act.getActType()).getCount() ;
						}	
						ActType newActInfo = ActType.newBuilder()
								.setActType(act.getActType())
								.setCount(count)
								.setLastUpdateTime(act.getLastUpdateTime())
								.build();
						
						actMap.put(act.getActType(), newActInfo);
					}
					itemMap.put(item.getItem(), actMap);
				}
				detailMap.put(tsegs.getTimeId(), itemMap);
			}
		}
		
		private void mergeNewRecordsToMap(Long timeId, String itemId, ActiveRecord activeRecord,
				HashMap<Long,HashMap<String,HashMap<Integer,ActType>>> detailMap ) {
			
			HashMap<String,HashMap<Integer,ActType>> itemMap;
			if(detailMap.containsKey(timeId)){
				itemMap = detailMap.get(timeId);
			}else{				
				itemMap = new HashMap<String,HashMap<Integer,ActType>>();
			}
			
			HashMap<Integer,ActType> actMap;
			if(itemMap.containsKey(itemId)){
				actMap = itemMap.get(itemId);
			}else{
				actMap = new HashMap<Integer,ActType>();		
			}	
				
			long count = 1L;
			if(actMap.containsKey(activeRecord.getActType())){
				count = count + actMap.get(activeRecord.getActType()).getCount() ;
			}
			ActType newActInfo = ActType.newBuilder()
					.setActType(activeRecord.getActType())
					.setCount(count)
					.setLastUpdateTime(activeRecord.getActTime())
					.build();
			
			actMap.put(activeRecord.getActType(), newActInfo);
			itemMap.put(itemId, actMap);
			detailMap.put(timeId, itemMap);
		}
		
		private void mergeToHeap(
				ActionCombinerValue newValueList,
				UserActiveDetail oldValueHeap,
				UserActiveDetail.Builder mergeValueBuilder) {
			
			HashMap<Long,HashMap<String,HashMap<Integer,ActType>>> detailMap
						= new HashMap<Long,HashMap<String,HashMap<Integer,ActType>>>();
			if(oldValueHeap != null && oldValueHeap.getTsegsCount()>0){
				mergeOldToMap(oldValueHeap,detailMap);
				if(debug){
					logger.info("add old values,now size="+oldValueHeap.getTsegsCount());
				}
			}
			
			for(String item:newValueList.getActRecodeMap().keySet()){
				ActiveRecord action = newValueList.getActRecodeMap().get(item);
				Long winId = Utils.getDateByTime(action.getActTime());				
				mergeNewRecordsToMap(winId,item,action,detailMap);
				if(debug){
					logger.info("add new values,size="+detailMap.get(winId).size());
				}
			}	
			changeMapToPB(detailMap,mergeValueBuilder);
		}
				
		private void changeMapToPB(
				HashMap<Long, HashMap<String, HashMap<Integer, ActType>>> detailMap,
				Builder mergeValueBuilder) {
			Long time = System.currentTimeMillis()/1000;

			ArrayList<Long> sortList = new ArrayList<Long>(detailMap.keySet());
			
			Collections.sort(sortList, new Comparator<Long>() {   
				@Override
				public int compare(Long arg0,Long arg1) {
					return (int)(arg1.longValue() - arg0.longValue());
				}
			}); 
			
			for(Long timeId: sortList){
				if(timeId < Utils.getDateByTime(time - dataExpireTime)){
					continue;
				}
				
				Recommend.UserActiveDetail.TimeSegment.Builder timeBuilder =
									Recommend.UserActiveDetail.TimeSegment.newBuilder();
				timeBuilder.setTimeId(timeId);
				int count=0;
				for(String item:detailMap.get(timeId).keySet()){
					count++;
					if(count > topNum){
						break;
					}
					
					Recommend.UserActiveDetail.TimeSegment.ItemInfo.Builder itemBuilder =
									Recommend.UserActiveDetail.TimeSegment.ItemInfo.newBuilder();
					itemBuilder.setItem(item);
					for(Integer act: detailMap.get(timeId).get(item).keySet()){
						ActType actValue = detailMap.get(timeId).get(item).get(act);
						itemBuilder.addActs(actValue);
					}
					timeBuilder.addItems(itemBuilder);
				}
				mergeValueBuilder.addTsegs(timeBuilder.build());
			}
		}

		private void printOut(Recommend.UserActiveDetail mergeValue){	
			for(UserActiveDetail.TimeSegment tsegs: mergeValue.getTsegsList()){	
				logger.info("--id="+tsegs.getTimeId());
				for(UserActiveDetail.TimeSegment.ItemInfo item: tsegs.getItemsList()){	
					logger.info("-------item="+item.getItem());
					for(UserActiveDetail.TimeSegment.ItemInfo.ActType act: item.getActsList()){
						logger.info("-----------act="+act.getActType()+",count="+act.getCount()+",time="+act.getLastUpdateTime());
					}
				}
			}
		}
		
		private void Save(Recommend.UserActiveDetail.Builder mergeValueBuilder){	
			if(debug){
				printOut(mergeValueBuilder.build());
			}

			Future<Result<Void>> future = null;
			UserActiveDetail pbValue = mergeValueBuilder.build();		
			
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
				try {
					future = clientEntry.getClient().putAsync((short)nsTableId, key.getBytes(),pbValue.toByteArray(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,key,pbValue.toByteArray(),putopt));
				} catch (Exception e){
					logger.error(e.getMessage(), e);
				}
				break;
			}
		}
			
		@Override
		public void handle(Future<?> future, Object context) {			
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			UserActiveDetail.Builder mergeValueBuilder = Recommend.UserActiveDetail.newBuilder();

			UserActiveDetail oldValueHeap = null;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() != null){
					oldValueHeap = Recommend.UserActiveDetail.parseFrom(res.getResult());
				}				
			} catch (Exception e) {	
				logger.error(e.getMessage(), e);
			}
			
			if(debug){
				if(oldValueHeap == null){
					logger.info("get key="+key+",from tde ,but null");
				}else{
					logger.info("get key="+key+",from tde success");
				}
			}
			mergeToHeap(values,oldValueHeap,mergeValueBuilder);
			Save(mergeValueBuilder);
		}
	}

}