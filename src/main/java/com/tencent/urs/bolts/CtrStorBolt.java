package com.tencent.urs.bolts;

import java.lang.ref.SoftReference;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

import com.tencent.urs.protobuf.Recommend.ActiveType;
import com.tencent.urs.protobuf.Recommend.CtrInfo;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.Utils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;


public class CtrStorBolt extends AbstractConfigUpdateBolt{

	private static final long serialVersionUID = 1L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private UpdateCallBack putCallBack;
	private ConcurrentHashMap<String, CtrCombinerValue> combinerMap;

	private DataCache<CtrInfo> ctrCache;
	private int nsTableId;
	private int dataExpireTime;
	private int cacheExpireTime;
		
	class CtrCombinerValue{
		private Long click;
		private Long impress;
		private Long lastUpdateTime;
		
		public CtrCombinerValue(ActiveType type,Long count,Long time){
			if(ActiveType.Impress == type){
				click = 0L;
				impress = count;
			}else if(ActiveType.Click == type){
				impress = 0L;
				click = count;
			}
			lastUpdateTime = time;
		}
		
		public Long getTime(){
			return this.lastUpdateTime;
		}
		
		public Long getClick(){
			return this.click;
		}
		
		public Long getImpress(){
			return this.impress;
		}

		public void incrument(CtrCombinerValue newValue) {
			this.click = click + newValue.getClick();
			this.impress = impress + newValue.getImpress();
			this.lastUpdateTime = newValue.getTime();
		}
	}
	
	private static Logger logger = LoggerFactory.getLogger(CtrStorBolt.class);
	
	public CtrStorBolt(String config, ImmutableList<Output> outputField) {
		super(config, outputField, Constants.config_stream);
	}

	@Override
	public void updateConfig(XMLConfiguration config) {
		nsTableId = config.getInt("storage_table",517);
		dataExpireTime = config.getInt("data_expiretime",7*24*3600);
		cacheExpireTime = config.getInt("cache_expiretime",3600);
		//topNum = config.getInt("topNum",30);
	}
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		super.prepare(conf, context, collector);
		updateConfig(super.config);

		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.combinerMap = new ConcurrentHashMap<String,CtrCombinerValue>(1024);
		this.putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_interfaceID, this.getClass().getName());
		this.ctrCache = new DataCache<CtrInfo>(conf);
		
		int combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5);
		setCombinerTime(combinerExpireTime);
	}	

	@Override
	public void processEvent(String sid, Tuple tuple) {
		try{
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
				String[] items = actionResult.split(";",-1);
				
				if(Utils.isPageIdValid(pageId)){
					for(String eachItem: items){
						if(Utils.isItemIdValid(eachItem)){						
							StringBuffer getKey = new StringBuffer(bid);		
							getKey.append("#").append(adpos).append("#").append(pageId).append("#").append(eachItem);	

							//logger.info("add items ,getKey = "+getKey.toString()+",value="+actionType+",time="+actionTime);
							CtrCombinerValue vlaue = new CtrCombinerValue(actType,1L, Long.valueOf(actionTime));
							combinerKeys(getKey.toString(),vlaue);
						}
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
						Set<String> keySet = combinerMap.keySet();
						for (String key : keySet) {
							 CtrCombinerValue expireTimeValue = combinerMap.remove(key);
							try{
								new CtrUpdateCallBack(key,expireTimeValue).excute();
							}catch(Exception e){
								//mt.addCountEntry(systemID, interfaceID, item, count)
							}
						}
					}
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
				}
			}
		}).start();
	}
	
	private void combinerKeys(String key,CtrCombinerValue value) {
		synchronized(combinerMap){
			if(combinerMap.containsKey(key)){
				CtrCombinerValue oldValue = combinerMap.get(key);
				oldValue.incrument(value);
				combinerMap.put(key, oldValue);
			}else{
				combinerMap.put(key, value);
			}	
		}
	}	

	private class CtrUpdateCallBack implements MutiClientCallBack{
		private final String key;
		private final CtrCombinerValue values;

		public CtrUpdateCallBack(String key, CtrCombinerValue values) {
			this.key = key ; 
			this.values = values;		
		}
	
		public void excute() {
			try {			
				CtrInfo oldCtr = null;
				SoftReference<CtrInfo> sr = ctrCache.get(key);
				if(sr != null){
					oldCtr = sr.get();
				}
				
				if(oldCtr != null){	
					mergeHeap(oldCtr);
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsTableId,key.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}			
				
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
					
		@Override
		public void handle(Future<?> future, Object context) {			
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			CtrInfo oldCtr = null;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() !=null){
					oldCtr = CtrInfo.parseFrom(res.getResult());		
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}	
			mergeHeap(oldCtr);
		}

		private void mergeHeap(CtrInfo oldCtr) {
			CtrInfo.Builder mergeBuilder = CtrInfo.newBuilder();
			CtrInfo.TimeSegment.Builder newValueBuilder  = CtrInfo.TimeSegment.newBuilder();
			newValueBuilder.setClick(values.getClick()).setImpress(values.getImpress());
			
			boolean insert_flag = false;
			if(oldCtr != null){
				for(CtrInfo.TimeSegment ts:oldCtr.getTsegsList()){
					if(ts.getTimeId() == Utils.getDateByTime(values.getTime())){
						newValueBuilder.setTimeId(ts.getTimeId());
						newValueBuilder.setClick(values.getClick()+ts.getClick()).setImpress(values.getImpress()+ts.getImpress());
						mergeBuilder.addTsegs(newValueBuilder.build());
						insert_flag = true;
					}else if(ts.getTimeId() >= Utils.getDateByTime(values.getTime() - dataExpireTime)){
						mergeBuilder.addTsegs(ts);
					}
				}
			}

			if(!insert_flag){
				newValueBuilder.setTimeId(Utils.getDateByTime(values.getTime()));
				mergeBuilder.addTsegs(newValueBuilder.build());
			}
			Save(mergeBuilder.build());
		}
		
		private void Save(CtrInfo value){	
			synchronized(ctrCache){
				ctrCache.set(key, new SoftReference<CtrInfo>(value), cacheExpireTime);
			}
			
			/*
			for(CtrInfo.TimeSegment ts: value.getTsegsList()){
				logger.info("result,key="+key+",time="+ts.getTimeId()+",click="+ts.getClick()+",impress="+ts.getImpress()+",tableId="+nsTableId);
			}*/
				
			
			Future<Result<Void>> future = null;
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
				try {
					future = clientEntry.getClient().putAsync((short)nsTableId, key.getBytes(),value.toByteArray(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,key,value.toByteArray(),putopt));
					
					/*
					if(mt!=null){
						MonitorEntry mEntryPut = new MonitorEntry(Constants.SUCCESSCODE,Constants.SUCCESSCODE);
						mEntryPut.addExtField("TDW_IDC", clientEntry.getGroupname());
						mEntryPut.addExtField("tbl_name", "FIFO1");
						mt.addCountEntry(Constants.systemID, Constants.tde_put_interfaceID, mEntryPut, 1);
					}*/
				} catch (Exception e){
					logger.error(e.getMessage(), e);
				}
			}
		}
	}
	
}