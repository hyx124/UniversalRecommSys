package com.tencent.urs.statistics;

import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.ActiveType;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.Builder;
import com.tencent.urs.protobuf.Recommend.UserActiveHistory.ActiveRecord;

import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;

import com.tencent.monitor.MonitorEntry;
import com.tencent.monitor.MonitorTools;

import com.tencent.tde.client.Result;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.error.TairFlowLimit;
import com.tencent.tde.client.error.TairQueueOverflow;
import com.tencent.tde.client.error.TairRpcError;
import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.algorithms.AlgAdpter;
import com.tencent.urs.asyncupdate.UpdateCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBackContext;
import com.tencent.urs.combine.ActionCombinerValue;
import com.tencent.urs.conf.AlgModuleConf.AlgModuleInfo;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.Utils;

public class UserActionHandler implements AlgAdpter{
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private DataCache<HashMap<Long,HashMap<String,HashMap<Recommend.ActiveType,ActType>>>> cacheMap;
	private UpdateCallBack putCallBack;
	private ConcurrentHashMap<String, ActionCombinerValue> combinerMap;
	private int nsTableID;
	private int combinerExpireTime;
	
	private AlgModuleInfo algInfo;
	
	private static Logger logger = LoggerFactory
			.getLogger(UserActionHandler.class);
	
	private void setCombinerTime(final int second, final AlgAdpter bolt) {
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
		synchronized (combinerMap) {
			if(combinerMap.containsKey(key)){
				ActionCombinerValue oldvalue = combinerMap.get(key);
				value.incrument(oldvalue);
			}
			combinerMap.put(key, value);
		}
	}	

	@SuppressWarnings("rawtypes")
	public UserActionHandler(Map conf){
		this.nsTableID = Utils.getInt(conf, "tableid", 11);
		
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.cacheMap = new DataCache<HashMap<Long,HashMap<String,HashMap<Recommend.ActiveType,ActType>>>>(conf);
		this.combinerMap = new ConcurrentHashMap<String,ActionCombinerValue>(1024);
		this.putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_interfaceID, this.getClass().getName());
		
		this.combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5);
		setCombinerTime(combinerExpireTime, this);
	}

	private class ActionDetailUpdateAysncCallback implements MutiClientCallBack{
		private final String key;
		private final ActionCombinerValue values;

		public ActionDetailUpdateAysncCallback(String key, ActionCombinerValue values) {
			this.key = key ; 
			this.values = values;								
		}

		public void excute() {
			try {
				if(cacheMap.hasKey(key)){		
					SoftReference<HashMap<Long, HashMap<String, HashMap<ActiveType, ActType>>>> detailMap = cacheMap.get(key);	
					mergeNewValueToMap(values,detailMap.get());
					Save(key,detailMap.get());
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsTableID,key.getBytes(),opt);
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
		
		private void addOldRecordsToMap(UserActiveDetail oldValList,
				HashMap<Long,HashMap<String,HashMap<Recommend.ActiveType,ActType>>> detailMap ){
			Long expireTimeId =  System.currentTimeMillis()/1000;
			
			for(UserActiveDetail.TimeSegment tsegs: oldValList.getTsegsList()){
				if(tsegs.getTimeId() <expireTimeId){
					continue;
				}
				
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
						if(actMap.containsKey(act)){
							count = count + actMap.get(act).getCount() ;
						}	
						ActType newActInfo = ActType.newBuilder()
								.setActType(act.getActType())
								.setCount(count)
								.setLastUpdateTime(act.getLastUpdateTime())
								.build();
						
						actMap.put(act.getActType(), newActInfo);
					}
				}
			}
			
		}
		
		private void mergeNewRecordsToMap(String itemId, ActiveRecord activeRecord,
				HashMap<Long,HashMap<String,HashMap<Recommend.ActiveType,ActType>>> detailMap ) {

			Long nowTimeId = System.currentTimeMillis()/1000;
			
			HashMap<String,HashMap<Recommend.ActiveType,ActType>> itemMap;
			if(detailMap.containsKey(nowTimeId)){
				itemMap = detailMap.get(nowTimeId);
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
			detailMap.put(nowTimeId, itemMap);
		}
		
		private void mergeNewValueToMap(ActionCombinerValue newValList,
				HashMap<Long,HashMap<String,HashMap<Recommend.ActiveType,ActType>>> detailMap){						
			for(String item:newValList.getActRecodeMap().keySet()){
				mergeNewRecordsToMap(item,newValList.getActRecodeMap().get(item),detailMap);
			}
		}
		
		private void putToBuilder(
				HashMap<Long, HashMap<String, HashMap<ActiveType, ActType>>> detailMap,
				Builder pbValueBuilder) {
			for(Long time: detailMap.keySet()){
				Recommend.UserActiveDetail.TimeSegment.Builder timeBuilder =
						Recommend.UserActiveDetail.TimeSegment.newBuilder();
				timeBuilder.setTimeId(time);
				
				for(String item:detailMap.get(time).keySet()){
					Recommend.UserActiveDetail.TimeSegment.ItemInfo.Builder itemBuilder =
							Recommend.UserActiveDetail.TimeSegment.ItemInfo.newBuilder();
					itemBuilder.setItem(item);
					
					for(ActiveType act: detailMap.get(time).get(item).keySet()){
						ActType actValue = detailMap.get(time).get(item).get(act);
						itemBuilder.addActs(actValue);
					}
					timeBuilder.addItems(itemBuilder);
				}
				pbValueBuilder.addTsegs(timeBuilder.build());
			}
		}
		
		private void Save(String key,HashMap<Long, HashMap<String, HashMap<ActiveType, ActType>>> detailMap){	
			Future<Result<Void>> future = null;
			
			UserActiveDetail.Builder pbValueBuilder = UserActiveDetail.newBuilder();
			putToBuilder(detailMap,pbValueBuilder);
			
			synchronized(cacheMap){
				cacheMap.set(key, 
							new SoftReference<HashMap<Long, HashMap<String, HashMap<ActiveType, ActType>>>>(detailMap), 
							algInfo.getCacheExpireTime());
			}
			
			
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, algInfo.getDataExpireTime());
				try {
					UserActiveDetail putValue = pbValueBuilder.build();					
					future = clientEntry.getClient().putAsync((short)algInfo.getOutputTableId(), key.getBytes(),putValue.toByteArray(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,key,putValue.toByteArray(),putopt));
					
					
					if(mt!=null){
						MonitorEntry mEntryPut = new MonitorEntry(Constants.SUCCESSCODE,Constants.SUCCESSCODE);
						mEntryPut.addExtField("TDW_IDC", clientEntry.getGroupname());
						mEntryPut.addExtField("tbl_name", "FIFO1");
						mt.addCountEntry(Constants.systemID, Constants.tde_put_interfaceID, mEntryPut, 1);
					}
				} catch (Exception e){
					logger.error(e.toString());
				}
			}
		}
	
		@Override
		public void handle(Future<?> future, Object context) {			
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			byte[] oldVal = null;
			try {
				oldVal = afuture.get().getResult();
				UserActiveDetail oldValueHeap = Recommend.UserActiveDetail.parseFrom(oldVal);
				HashMap<Long, HashMap<String, HashMap<ActiveType, ActType>>> detailMap = 
						new HashMap<Long, HashMap<String, HashMap<ActiveType, ActType>>>();	

				addOldRecordsToMap(oldValueHeap,detailMap);
				mergeNewValueToMap(values,detailMap);
				Save(key,detailMap);
			} catch (Exception e) {
				
			}
			
		}
	}

	private ActionCombinerValue genCombinerValue(Tuple input){
		return null;
	}
	
	@Override
	public void deal(AlgModuleInfo algInfo,Tuple input) {
		String key = input.getStringByField("qq");
		ActionCombinerValue value = genCombinerValue(input);
		this.algInfo = algInfo;
		combinerKeys(key,value);	
	}


}