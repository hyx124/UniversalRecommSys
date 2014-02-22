package com.tencent.urs.bolts;

import com.google.common.collect.ImmutableList;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.ActiveType;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType;

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
import com.tencent.tde.client.impl.MutiThreadCallbackClient;
import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBackContext;
import com.tencent.urs.combine.GroupActionCombinerValue;
import com.tencent.urs.combine.UpdateKey;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.Utils;

public class ItemCountBolt2 extends AbstractConfigUpdateBolt{
	private static final long serialVersionUID = 2972911860800045348L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private DataCache<Float> groupCountCache;
	private DataCache<Float> userCountCache;

	private ConcurrentHashMap<UpdateKey, GroupActionCombinerValue> combinerMap;
	private ConcurrentHashMap<Recommend.ActiveType, Float> actWeightMap;
	private Long lastUpdateTime; 
	private UpdateCallBack putCallBack;	
	
	private int nsUserCountTableId;
	private int nsGroupCountTableId;
	private int nsDetailTableId;
	private int nsActWeightTableId;
	
	private int dataExpireTime;
	private int cacheExpireTime;
	private boolean debug;
	
	public ItemCountBolt2(String config, ImmutableList<Output> outputField) {
		super(config, outputField, Constants.config_stream);
	}

	@Override
	public void updateConfig(XMLConfiguration config) {
		nsUserCountTableId = config.getInt("user_count_table",303);
		nsGroupCountTableId = config.getInt("group_count_table",304);
		nsDetailTableId = config.getInt("dependent_table",302);
		
		nsActWeightTableId = config.getInt("act_weight_table",314);
		dataExpireTime = config.getInt("data_expiretime",1*24*3600);
		cacheExpireTime = config.getInt("cache_expiretime",3600);
		debug = config.getBoolean("debug",false);
	}
	
	private void weightInit(){
		actWeightMap.put(Recommend.ActiveType.Impress, 0.5F);
		actWeightMap.put(Recommend.ActiveType.Click, 1F);
		actWeightMap.put(Recommend.ActiveType.PageView, 1F);
		actWeightMap.put(Recommend.ActiveType.Read, 1.5F);
		actWeightMap.put(Recommend.ActiveType.Save, 2F);
		actWeightMap.put(Recommend.ActiveType.BuyCart, 2F);
		actWeightMap.put(Recommend.ActiveType.Deal, 2F);
		actWeightMap.put(Recommend.ActiveType.Score, 3F);
		actWeightMap.put(Recommend.ActiveType.Comments, 3F);
		actWeightMap.put(Recommend.ActiveType.Reply, 3F);
		actWeightMap.put(Recommend.ActiveType.Ups, 3F);
		actWeightMap.put(Recommend.ActiveType.Praise, 4F);
		actWeightMap.put(Recommend.ActiveType.Share, 4F);
	}
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		super.prepare(conf, context, collector);
		updateConfig(super.config);
	
		this.lastUpdateTime = 0L;
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.groupCountCache = new DataCache<Float>(conf);
		this.userCountCache = new DataCache<Float>(conf);
		
		this.actWeightMap = new ConcurrentHashMap<Recommend.ActiveType, Float>(20);
		
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
			.getLogger(ItemCountBolt2.class);
	
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
	
	private Float getWeightByType(Recommend.ActiveType actionType){
		
		/*Long now = System.currentTimeMillis()/1000L;
		if(lastUpdateTime == 0){
			weightInit();
			lastUpdateTime = now;
		}else if(lastUpdateTime < (now - 3600*24)  ){
			actWeightMap.clear();
			lastUpdateTime = now;
		}*/
		weightInit();
		if(actWeightMap.containsKey(actionType)){
			return actWeightMap.get(actionType);
		}else{
			try{
				MutiThreadCallbackClient clientEntry = mtClientList.get(0).getClient();
				TairOption opt = new TairOption(mtClientList.get(0).getTimeout());
				Result<byte[]> res =  clientEntry.get((short)nsActWeightTableId, actionType.toString().getBytes(), opt);
				if(res.isSuccess() && res.getResult()!=null)
				{
						
					Recommend.ActionWeightInfo pbWeightInfo = Recommend.ActionWeightInfo.parseFrom(res.getResult());
					if(pbWeightInfo.getWeight()>=0){
						actWeightMap.put(actionType, pbWeightInfo.getWeight());
						return pbWeightInfo.getWeight();
					}
				}					
			}catch(Exception e){
				logger.error(e.toString());
			}
		}
		return 0F;
	}

	private class GroupCountUpdateCallback implements MutiClientCallBack{
		private final Float value;
		private final String putKey;

		public GroupCountUpdateCallback(UpdateKey key, Float value) {
			this.value = value;		
			this.putKey = key.getGroupCountKey();
		}
		
		public void excute() {
			try {			
				Float oldValue = null;
				SoftReference<Float> sr = groupCountCache.get(putKey);
				if(sr != null){
					oldValue = sr.get();
				}
				
				if(oldValue != null){		
					Float newValue = oldValue+value;
					logger.info("get step3 key="+putKey+",group old count="+oldValue+",new count="+this.value+",merge count="+newValue);
					Save(putKey,newValue);
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsGroupCountTableId,putKey.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}			
				
			} catch (Exception e){
				logger.error(e.getMessage(), e);
			}
		}

		private void Save(String key,Float value){		
			synchronized(groupCountCache){
				groupCountCache.set(key, new SoftReference<Float>(value), cacheExpireTime);
			}
			
			logger.info("step3 save pair count,key="+key+",value="+value);
			Future<Result<Void>> future = null;
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
					logger.error(e.getMessage(), e);
				}
			}
		}
		
		@Override
		public void handle(Future<?> future, Object context) {
			// TODO Auto-generated method stub
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			Float newValue = 0F;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() != null){
					String oldValue = new String(res.getResult());
					newValue = this.value + Float.parseFloat(oldValue);
					Save(putKey,newValue);
					return;
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
			Save(putKey,this.value);
		}
		
	}
	
	private class UserCountUpdateCallBack implements MutiClientCallBack{
		
		private UpdateKey key;
		private Float count;
		private String userCountKey;

		public UserCountUpdateCallBack(UpdateKey key, Float count) {
			this.key = key ; 
			this.count = count;	
			this.userCountKey = key.getUserCountKey();
		}
		
		public void excute() {
			try {
				Float oldCount = null;
				SoftReference<Float> sr = userCountCache.get(userCountKey);
				if(sr != null){
					oldCount = sr.get();
				}
				
				if(oldCount != null){		
					next(oldCount);
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsUserCountTableId,userCountKey.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}			
				
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
		
		public void next(Float oldCount){
			logger.info("get step2 key="+userCountKey+",old count="+oldCount+",new count="+count);
			if(count != oldCount){
				new GroupCountUpdateCallback(key,count-oldCount).excute();
				
				UpdateKey noGroupKey = new UpdateKey(key.getBid(),key.getUin(),0,key.getAdpos(),key.getItemId());
				new GroupCountUpdateCallback(noGroupKey,count-oldCount).excute();
				
				save(userCountKey,count);
			}
			
		}
		
		private void save(String userCountKey,Float count){
			Future<Result<Void>> future = null;
			synchronized(userCountCache){
				userCountCache.set(userCountKey, new SoftReference<Float>(count), cacheExpireTime);
			}
			
			logger.info("step2,save usercount,key="+userCountKey+",value="+count);
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
					logger.error(e.getMessage(), e);
				}
			}
		}
			
		@Override
		public void handle(Future<?> future, Object arg1) {
			// TODO Auto-generated method stub
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			Float oldCount = 0F;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() != null){
					oldCount = Float.parseFloat(new String(res.getResult()));
				}				
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
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

		public void next(Float weight){
			if(debug){
				logger.info("step1, key="+userCheckKey+",new count="+weight);
			}
			
			if(weight>0){
				new UserCountUpdateCallBack(key,weight).excute();
			}
		}
		
		public void excute() {
			try {
				ClientAttr clientEntry = mtClientList.get(0);		
				TairOption opt = new TairOption(clientEntry.getTimeout());
				Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsDetailTableId,userCheckKey.getBytes(),opt);
				clientEntry.getClient().notifyFuture(future, this,clientEntry);	
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
			
		private Float getWeight(Recommend.UserActiveDetail oldValueHeap){						
			Float newWeight = getWeightByType(this.values.getType());					
			for(TimeSegment ts:oldValueHeap.getTsegsList()){
				if(ts.getTimeId() < getWinIdByTime(values.getTime() - dataExpireTime)){
					continue;
				}
			
				for(ItemInfo item:ts.getItemsList()){						
					if(item.getItem().equals(key.getItemId())){	
						for(ActType act: item.getActsList()){	
							if(act.getLastUpdateTime() > (values.getTime() - dataExpireTime)){
								Float actWeight = getWeightByType(act.getActType());
								newWeight =  Math.max(newWeight,actWeight);
								logger.info("timeId="+ts.getTimeId()+",type="+act.getActType()+",weight="+newWeight);
							}
						}	
					}					
				}
			}
			logger.info("item="+key.getItemId()+",final weight="+newWeight);
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
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() !=null){
					Recommend.UserActiveDetail oldValueHeap = Recommend.UserActiveDetail.parseFrom(res.getResult());
					next(getWeight(oldValueHeap));
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}	
		}
	}
	
}