package com.tencent.urs.bolts;

import com.google.common.collect.ImmutableList;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.ActiveType;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType;

import java.lang.ref.SoftReference;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.apache.commons.configuration.ConfigurationException;
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
import com.tencent.urs.combine.GroupActionCombinerValue;
import com.tencent.urs.combine.UpdateKey;
import com.tencent.urs.conf.AlgModuleConf;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.Utils;

public class ItemCountBolt extends AbstractConfigUpdateBolt{
	private static final long serialVersionUID = 2972911860800045348L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private DataCache<Recommend.UserActiveDetail> userActionCache;
	private DataCache<Integer> groupCountCache;
	private DataCache<Integer> userCountCache;
	private ConcurrentHashMap<UpdateKey, GroupActionCombinerValue> combinerMap;
	private UpdateCallBack putCallBack;	
	
	private int nsUserCountTableId;
	private int nsGroupCountTableId;
	private int nsDetailTableId;
	private int dataExpireTime;
	private int cacheExpireTime;
	//private int topNum;
	
	public ItemCountBolt(String config, ImmutableList<Output> outputField) {
		super(config, outputField, Constants.config_stream);
	}

	@Override
	public void updateConfig(XMLConfiguration config) {
		nsUserCountTableId = config.getInt("user_count_table",303);
		nsGroupCountTableId = config.getInt("group_count_table",304);
		nsDetailTableId = config.getInt("dependent_table",302);
		dataExpireTime = config.getInt("data_expiretime",1*24*3600);
		cacheExpireTime = config.getInt("cache_expiretime",3600);
		//topNum = config.getInt("topNum",30);
	}
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		super.prepare(conf, context, collector);
		updateConfig(super.config);
	
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.userActionCache = new DataCache<Recommend.UserActiveDetail>(conf);
		this.groupCountCache = new DataCache<Integer>(conf);
		this.userCountCache = new DataCache<Integer>(conf);
		this.combinerMap = new ConcurrentHashMap<UpdateKey,GroupActionCombinerValue>(1024);
				
		this.putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_interfaceID, this.getClass().getName());
		
		int combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5);
		setCombinerTime(combinerExpireTime);
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {	
		String bid = tuple.getStringByField("bid");
		String qq = tuple.getStringByField("qq");
		String groupId = tuple.getStringByField("group_id");
		String adpos = tuple.getStringByField("adpos");
		String itemId = tuple.getStringByField("item_id");
		
		String actionType = tuple.getStringByField("action_type");
		String actionTime = tuple.getStringByField("action_time");
	
		ActiveType actType = Utils.getActionTypeByString(actionType);
		
		if(!Utils.isBidValid(bid) || !Utils.isQNumValid(qq) || !Utils.isGroupIdVaild(groupId) || !Utils.isItemIdValid(itemId)){
			return;
		}
		
		GroupActionCombinerValue value = new GroupActionCombinerValue(actType,Long.valueOf(actionTime));
		UpdateKey key = new UpdateKey(bid,Long.valueOf(qq),Integer.valueOf(groupId),adpos,itemId);
		combinerKeys(key,value);
	}
	
	private static Logger logger = LoggerFactory
			.getLogger(ItemCountBolt.class);
	
	private void setCombinerTime(final int second) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					while (true) {
						Thread.sleep(second * 1000);
						Set<UpdateKey> keySet = combinerMap.keySet();
						for (UpdateKey key : keySet) {
							GroupActionCombinerValue expireTimeValue  = combinerMap.remove(key);
							try{
								new ActionDetailCallBack(key,expireTimeValue).excute();
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
	
	private void combinerKeys(UpdateKey key,GroupActionCombinerValue value) {
		synchronized(combinerMap){
			if(combinerMap.containsKey(key)){
				GroupActionCombinerValue oldValue = combinerMap.get(key);
				oldValue.incrument(value);
				combinerMap.put(key, oldValue);
			}else{
				combinerMap.put(key, value);
			}
			
		}
	}	

	private class GroupCountUpdateCallback implements MutiClientCallBack{
		private final Integer value;
		private final String putKey;

		public GroupCountUpdateCallback(UpdateKey key, Integer value) {
			this.value = value;		
			this.putKey = key.getGroupCountKey();
		}
		
		public void excute() {
			try {
				if(groupCountCache.hasKey(putKey)){		
					SoftReference<Integer> oldValue = groupCountCache.get(putKey);	
					Integer newValue = oldValue.get()+value;
					logger.info("get step3 key="+putKey+",group old count="+oldValue.get()+",new count="+this.value+",merge count="+newValue);

					Save(putKey,newValue);
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsGroupCountTableId,putKey.getBytes(),opt);
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

		private void Save(String key,Integer value){	
			
			Future<Result<Void>> future = null;
			synchronized(groupCountCache){
				groupCountCache.set(key, new SoftReference<Integer>(value), cacheExpireTime);
			}
			
			logger.info("key="+key+",value="+value);
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
				try {
					future = clientEntry.getClient().putAsync((short)nsGroupCountTableId, key.getBytes(),String.valueOf(value).getBytes(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,key,String.valueOf(value).getBytes(),putopt));
					
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
			// TODO Auto-generated method stub
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			int newValue = 0;
			try {
				String oldValue = new String(afuture.get().getResult());
				newValue = this.value + Integer.valueOf(oldValue);
				logger.info("get step3 key="+putKey+",group old count="+oldValue+",new count="+this.value+",merge count="+newValue);

				Save(putKey,newValue);
			} catch (Exception e) {
				Save(putKey,this.value);
			}
			
		}
		
	}
	
	private class compareWeighCallBack implements MutiClientCallBack{
		
		private UpdateKey key;
		private Integer count;
		private String userCountKey;
		private Long time;

		public compareWeighCallBack(UpdateKey key, Integer count) {
			this.key = key ; 
			this.count = count;	
			//this.time = time;
			this.userCountKey = key.getUserCountKey();
		}
		
		public void excute() {
			try {
				if(userCountCache.hasKey(userCountKey)){		
					SoftReference<Integer> oldCount = userCountCache.get(userCountKey);	
					next(oldCount.get());
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsUserCountTableId,userCountKey.getBytes(),opt);
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
		
		public void next(Integer oldCount){
			logger.info("get step2 key="+userCountKey+",old count="+oldCount+",new count="+count);
			if(count > oldCount){
				new GroupCountUpdateCallback(key,count-oldCount).excute();
				
				UpdateKey noGroupKey = new UpdateKey(key.getBid(),key.getUin(),0,key.getAdpos(),key.getItemId());
				new GroupCountUpdateCallback(noGroupKey,count-oldCount).excute();
				
				save(userCountKey,count);
			}
			
		}
		
		private void save(String userCountKey,Integer count){
			Future<Result<Void>> future = null;
			synchronized(userCountCache){
				userCountCache.set(userCountKey, new SoftReference<Integer>(count), cacheExpireTime);
			}
			
			logger.info("key="+userCountKey+",value="+count);
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
				try {
					future = clientEntry.getClient().putAsync((short)nsUserCountTableId, userCountKey.getBytes(),String.valueOf(count).getBytes(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,userCountKey,String.valueOf(count).getBytes(),putopt));
					
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
		public void handle(Future<?> future, Object arg1) {
			// TODO Auto-generated method stub
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			Integer oldCount = 0;
			try {
				byte[] oldVal = afuture.get().getResult();
				oldCount = Integer.valueOf(new String(oldVal));
			} catch (Exception e) {
				
			}
			next(oldCount);	
		}
		
	}
	
	private class ActionDetailCallBack implements MutiClientCallBack{
		private final UpdateKey key;
		private final String userCheckKey;
		private final GroupActionCombinerValue values;

		public ActionDetailCallBack(UpdateKey key, GroupActionCombinerValue values) {
			this.key = key ; 
			this.values = values;		
			this.userCheckKey = key.getDetailKey();
		}

		public void next(Integer weight){
			logger.info("get step1 key="+userCheckKey+",new count="+weight);
			if(weight>0){
				new compareWeighCallBack(key,weight).excute();
			}
		}
		
		public void excute() {
			try {
				if(userActionCache.hasKey(userCheckKey)){		
					SoftReference<Recommend.UserActiveDetail> oldValueHeap = userActionCache.get(userCheckKey);	
					next(getWeight(oldValueHeap.get()));
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsDetailTableId,userCheckKey.getBytes(),opt);
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
			
		private Integer getWeight(Recommend.UserActiveDetail oldValueHeap){						
			Integer newWeight = 0;					
			for(TimeSegment ts:oldValueHeap.getTsegsList()){
				if(ts.getTimeId() < getWinIdByTime(values.getTime() - dataExpireTime)){
					continue;
				}
			
				for(ItemInfo item:ts.getItemsList()){						
					if(item.getItem().equals(key.getItemId())){	
						for(ActType act: item.getActsList()){	
							if(act.getLastUpdateTime() > (values.getTime() - dataExpireTime)){
								newWeight =  Math.max(newWeight,act.getActType().getNumber());
							}
						}	
					}					
				}
			}
			return newWeight;
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
				SoftReference<Recommend.UserActiveDetail> oldValueHeap = 
						new SoftReference<Recommend.UserActiveDetail>(Recommend.UserActiveDetail.parseFrom(oldVal));
				next(getWeight(oldValueHeap.get()));
				userActionCache.set(userCheckKey, oldValueHeap,cacheExpireTime);
			} catch (Exception e) {
				
			}
			
		}
	}
	
}