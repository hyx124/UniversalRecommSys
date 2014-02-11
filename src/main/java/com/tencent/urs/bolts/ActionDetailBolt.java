package com.tencent.urs.bolts;

import com.google.common.collect.ImmutableList;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.ActiveType;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.Builder;
import com.tencent.urs.protobuf.Recommend.UserActiveHistory.ActiveRecord;

import java.lang.ref.SoftReference;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
import com.tencent.tde.client.error.TairFlowLimit;
import com.tencent.tde.client.error.TairQueueOverflow;
import com.tencent.tde.client.error.TairRpcError;
import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBackContext;
import com.tencent.urs.combine.ActionCombinerValue;
import com.tencent.urs.combine.UpdateKey;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.Utils;

public class ActionDetailBolt extends AbstractConfigUpdateBolt{
	private static final long serialVersionUID = 5958754435166536530L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private DataCache<Recommend.UserActiveDetail> cacheMap;
	private UpdateCallBack putCallBack;
	private ConcurrentHashMap<String, ActionCombinerValue> combinerMap;

	private int nsTableId;
	private int dataExpireTime;
	private int cacheExpireTime;
	private int topNum;
	private boolean debug;
	
	private static Logger logger = LoggerFactory
			.getLogger(ActionDetailBolt.class);
	
	public ActionDetailBolt(String config, ImmutableList<Output> outputField) {
		super(config, outputField,Constants.config_stream);
	}

	@Override
	public void updateConfig(XMLConfiguration config) {
		nsTableId = config.getInt("storage_table",302);
		dataExpireTime = config.getInt("data_expiretime",1*24*3600);
		cacheExpireTime = config.getInt("cache_expiretime",3600);
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
		this.cacheMap = new DataCache<Recommend.UserActiveDetail>(conf);
		this.combinerMap = new ConcurrentHashMap<String,ActionCombinerValue>(1024);
		this.putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_interfaceID, "ActionDetail");
		
		int combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5);
		setCombinerTime(combinerExpireTime);
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {
		String bid = tuple.getStringByField("bid");
		String adpos = tuple.getStringByField("adpos");
		String qq = tuple.getStringByField("qq");
		String itemId = tuple.getStringByField("item_id");
		
		if(!Utils.isItemIdValid(itemId) || !Utils.isQNumValid(qq)){
			return;
		}
		
		String actionType = tuple.getStringByField("action_type");
		String actionTime = tuple.getStringByField("action_time");
		String lbsInfo = tuple.getStringByField("lbs_info");
		String platform = tuple.getStringByField("platform");
		
		ActiveType actType = Utils.getActionTypeByString(actionType);
		
		Recommend.UserActiveHistory.ActiveRecord.Builder actBuilder =
				Recommend.UserActiveHistory.ActiveRecord.newBuilder();
		actBuilder.setItem(itemId).setActTime(Long.valueOf(actionTime)).setActType(actType)
					.setLBSInfo(lbsInfo).setPlatForm(platform);

		ActionCombinerValue value = new ActionCombinerValue();
		value.init(itemId,actBuilder.build());
		UpdateKey key = new UpdateKey(bid, Long.valueOf(qq), 0, adpos, itemId);
		
		combinerKeys(key.getDetailKey(),value);	
	}
	
	private void setCombinerTime(final int second) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					while (true) {
						Thread.sleep(second * 1000);
						Set<String> keySet = combinerMap.keySet();
						for (String key : keySet) {
							ActionCombinerValue expireTimeValue  = combinerMap.remove(key);
							try{
								new ActionDetailUpdateAysncCallback(key,expireTimeValue).excute();
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
	
	private void combinerKeys(String key,ActionCombinerValue value) {
		synchronized(combinerMap){
			if(combinerMap.containsKey(key)){
				ActionCombinerValue oldValue = combinerMap.get(key);
				oldValue.incrument(value);
				combinerMap.put(key, oldValue);
			}else{
				combinerMap.put(key, value);
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
			Recommend.UserActiveDetail oldValue = null;
			SoftReference<UserActiveDetail> sr = cacheMap.get(key);
			if(sr != null){
				oldValue =  sr.get();
			}
						
			if(oldValue != null){	
				if(debug){
					logger.info("get key="+key+",in caceh");
				}
				UserActiveDetail.Builder mergeValueBuilder = Recommend.UserActiveDetail.newBuilder();
				mergeToHeap(values,oldValue,mergeValueBuilder);
				Save(mergeValueBuilder);
			}else{
				if(debug){
					logger.info("get key="+key+",in tde");
				}
				ClientAttr clientEntry = mtClientList.get(0);		
				TairOption opt = new TairOption(clientEntry.getTimeout());
				Future<Result<byte[]>> future;
				try {
					future = clientEntry.getClient().getAsync((short)nsTableId,key.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				} catch(Exception e){
					logger.error(e.toString());
				}
			
			}			

		}	
		
		private Long getWinIdByTime(Long time){	
			String expireId = new SimpleDateFormat("yyyyMMdd").format(time*1000L);
			return Long.valueOf(expireId);
		}
		
		private void mergeOldToMap(UserActiveDetail oldValList,
				HashMap<Long,HashMap<String,HashMap<Recommend.ActiveType,ActType>>> detailMap ){
			if(oldValList == null || oldValList.getTsegsCount() <=0 ){
				return;
			}
			
			for(UserActiveDetail.TimeSegment tsegs: oldValList.getTsegsList()){
				HashMap<String,HashMap<Recommend.ActiveType,ActType>> itemMap;		
				if(detailMap.containsKey(tsegs.getTimeId())){
					itemMap = detailMap.get(tsegs.getTimeId());					
				}else{				
					itemMap = new HashMap<String,HashMap<Recommend.ActiveType,ActType>>();
	
				}
				
				for(UserActiveDetail.TimeSegment.ItemInfo item: tsegs.getItemsList()){	
					HashMap<Recommend.ActiveType,ActType> actMap;
					if(itemMap.containsKey(item.getItem())){
						actMap = itemMap.get(item.getItem());
					}else{
						actMap = new HashMap<Recommend.ActiveType,ActType>();		
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
				HashMap<Long,HashMap<String,HashMap<Recommend.ActiveType,ActType>>> detailMap ) {
			
			HashMap<String,HashMap<Recommend.ActiveType,ActType>> itemMap;
			if(detailMap.containsKey(timeId)){
				itemMap = detailMap.get(timeId);
			}else{				
				itemMap = new HashMap<String,HashMap<Recommend.ActiveType,ActType>>();
			}
			
			HashMap<Recommend.ActiveType,ActType> actMap;
			if(itemMap.containsKey(itemId)){
				actMap = itemMap.get(itemId);
			}else{
				actMap = new HashMap<Recommend.ActiveType,ActType>();		
			}	
				
			Long count = 1L;
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
			
			HashMap<Long,HashMap<String,HashMap<Recommend.ActiveType,ActType>>> detailMap
						= new HashMap<Long,HashMap<String,HashMap<Recommend.ActiveType,ActType>>>();
			if(oldValueHeap != null && oldValueHeap.getTsegsCount()>0){
				mergeOldToMap(oldValueHeap,detailMap);
				if(debug){
					logger.info("add old values,now size="+oldValueHeap.getTsegsCount());
				}
			}
			
			for(String item:newValueList.getActRecodeMap().keySet()){
				ActiveRecord action = newValueList.getActRecodeMap().get(item);
				Long winId = getWinIdByTime(action.getActTime());				
				mergeNewRecordsToMap(winId,item,action,detailMap);
				if(debug){
					logger.info("add new values,size="+detailMap.get(winId).size());
				}
			}	
			changeMapToPB(detailMap,mergeValueBuilder);
		}
		
		private void changeMapToPB(
				HashMap<Long, HashMap<String, HashMap<ActiveType, ActType>>> detailMap,
				Builder mergeValueBuilder) {
			Long time = System.currentTimeMillis()/1000;
			for(Long timeId: detailMap.keySet()){
				if(timeId < getWinIdByTime(time - dataExpireTime) || timeId >getWinIdByTime(time)){
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
					for(ActiveType act: detailMap.get(timeId).get(item).keySet()){
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
			synchronized(cacheMap){
				cacheMap.set(key, new SoftReference<UserActiveDetail>(pbValue), cacheExpireTime);
			}			
			
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
				try {
					future = clientEntry.getClient().putAsync((short)nsTableId, key.getBytes(),pbValue.toByteArray(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,key,pbValue.toByteArray(),putopt));
					
					/*
					if(mt!=null){
						MonitorEntry mEntryPut = new MonitorEntry(Constants.SUCCESSCODE,Constants.SUCCESSCODE);
						mEntryPut.addExtField("TDW_IDC", clientEntry.getGroupname());
						mEntryPut.addExtField("tbl_name", "FIFO1");
						mt.addCountEntry(Constants.systemID, Constants.tde_put_interfaceID, mEntryPut, 1);
					}*/
				} catch (Exception e){
					logger.error(e.toString());
				}
			}
		}
			
		@Override
		public void handle(Future<?> future, Object context) {			
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			UserActiveDetail.Builder mergeValueBuilder = Recommend.UserActiveDetail.newBuilder();

			UserActiveDetail oldValueHeap = null;
			try {
				byte[] oldVal = afuture.get().getResult();
				oldValueHeap = Recommend.UserActiveDetail.parseFrom(oldVal);
			} catch (Exception e) {	
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