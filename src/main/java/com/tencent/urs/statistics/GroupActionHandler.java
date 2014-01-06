package com.tencent.urs.statistics;

import com.tencent.urs.protobuf.Recommend;
import java.lang.ref.SoftReference;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;

import com.tencent.monitor.MonitorTools;

import com.tencent.tde.client.Result;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.error.TairFlowLimit;
import com.tencent.tde.client.error.TairQueueOverflow;
import com.tencent.tde.client.error.TairRpcError;
import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.algorithms.AlgAdpter;
import com.tencent.urs.asyncupdate.UpdateCallBack;
import com.tencent.urs.combine.GroupActionCombinerValue;
import com.tencent.urs.combine.UpdateKey;
import com.tencent.urs.conf.AlgModuleConf.AlgModuleInfo;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.Utils;

public class GroupActionHandler implements AlgAdpter{
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private DataCache<Recommend.UserActiveDetail> userActionCache;
	private DataCache<Integer> groupCountCache;
	private ConcurrentHashMap<UpdateKey, GroupActionCombinerValue> combinerMap;
	private int nsTableID;
	private UpdateCallBack putCallBack;
	
	private AlgModuleInfo algInfo;
	
	private static Logger logger = LoggerFactory
			.getLogger(GroupActionHandler.class);
	
	private void setCombinerTime(final int second, final AlgAdpter bolt) {
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
								new ActionDetailCheckCallBack(key,expireTimeValue).excute();
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
				value.incrument(combinerMap.get(key));
			}
			combinerMap.put(key, value);
		}
	}	

	@SuppressWarnings("rawtypes")
	public GroupActionHandler(Map conf){
		this.nsTableID = Utils.getInt(conf, "tableid", 11);
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.userActionCache = new DataCache<Recommend.UserActiveDetail>(conf);
		this.groupCountCache = new DataCache<Integer>(conf);
		this.combinerMap = new ConcurrentHashMap<UpdateKey,GroupActionCombinerValue>(1024);
				
		
		this.putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_interfaceID, this.getClass().getName());
		
		int expireTime = Utils.getInt(conf, "expireTime",5*3600);
		setCombinerTime(expireTime, this);

	}

	private class GroupCountUpdateCallback implements MutiClientCallBack{
		private final UpdateKey key;
		private final Integer value;
		private final String putKey;

		public GroupCountUpdateCallback(UpdateKey key, Integer value) {
			this.key = key ; 
			this.value = value;		
			this.putKey = key.getItemId()+"#"+key.getAdpos()+"#"+key.getGroupId();
		}
		
		public void excute() {
			try {
				if(groupCountCache.hasKey(putKey)){		
					SoftReference<Integer> oldValue = groupCountCache.get(putKey);	
					SoftReference<Integer> newValue = new SoftReference<Integer>(oldValue.get()+value);
					Save(putKey,newValue);
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsTableID,putKey.getBytes(),opt);
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

		private void Save(String key,SoftReference<Integer> value){	
			groupCountCache.set(key, value, algInfo.getCacheExpireTime());
			
		}
		
		@Override
		public void handle(Future<?> future, Object context) {
			// TODO Auto-generated method stub
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			try {
				String oldValue = afuture.get().getResult().toString();
				SoftReference<Integer> oldValueInt = 
						new SoftReference<Integer>(Integer.valueOf(oldValue));
				SoftReference<Integer> newValue = new SoftReference<Integer>(oldValueInt.get()+value);
				Save(putKey,newValue);
			} catch (Exception e) {
				
			}
		}
		
	}
	
	private class ActionDetailCheckCallBack implements MutiClientCallBack{
		private final UpdateKey key;
		private final String userCheckKey;
		private final GroupActionCombinerValue values;

		public ActionDetailCheckCallBack(UpdateKey key, GroupActionCombinerValue values) {
			this.key = key ; 
			this.values = values;		
			this.userCheckKey = key.getUin() + "#" + algInfo.getAlgName();
		}

		public void next(Integer weight){
			new GroupCountUpdateCallback(key,weight).excute();
			
			UpdateKey noGroupKey = new UpdateKey(key.getUin(),0,key.getAdpos(),key.getItemId());
			new GroupCountUpdateCallback(noGroupKey,weight).excute();
		}
		
		public void excute() {
			try {
				if(userActionCache.hasKey(userCheckKey)){		
					SoftReference<Recommend.UserActiveDetail> oldValueHeap = userActionCache.get(userCheckKey);	
					next(getIncreasedWeight(oldValueHeap.get()));
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsTableID,userCheckKey.getBytes(),opt);
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
			
		private Integer getIncreasedWeight(Recommend.UserActiveDetail oldValueHeap){
			int lastMaxCount = 0;		
			int nowMaxCount = 0;			
			for(Recommend.UserActiveDetail.ActType act:oldValueHeap.getTypesList()){
				for(Recommend.UserActiveDetail.ActType.TimeSegment ts: act.getTsegsList()){
					if(justExpireSoon(ts.getTimeSegment())){
						for(Recommend.UserActiveDetail.ActType.TimeSegment.Item item:ts.getItemsList()){
							if(item.getItem().equals(key.getItemId())){
								lastMaxCount = Math.max(lastMaxCount, act.getActType().getNumber());
								
							}
						}
					}else if(justUpdateSoon(ts.getTimeSegment())){
						for(Recommend.UserActiveDetail.ActType.TimeSegment.Item item:ts.getItemsList()){
							if(item.getItem().equals(key.getItemId())){
								nowMaxCount = Math.max(nowMaxCount, act.getActType().getNumber());
							}
						}
					}
				}
			}
			return nowMaxCount - lastMaxCount;
		}

		private boolean justUpdateSoon(long timeSegment) {
			return false;
		}

		private boolean justExpireSoon(long timeSegment) {
			return false;
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
				next(getIncreasedWeight(oldValueHeap.get()));
			} catch (Exception e) {
				
			}
			
		}
	}

	@Override
	public void deal(AlgModuleInfo algInfo,Tuple input) {
		// TODO Auto-generated method stub	
		if(algInfo.getUpdateTime() > this.algInfo.getUpdateTime()){
			this.algInfo = algInfo;
		}
		
		Long uin = input.getLongByField("uin");
		Integer groupId = input.getIntegerByField("group_id");
		String adpos = input.getStringByField("adpos");
		String itemId = input.getStringByField("itemId");
		
		
		GroupActionCombinerValue value = new GroupActionCombinerValue();
		
		if(Utils.isQNumValid(uin) && Utils.isGroupIdVaild(groupId) && Utils.isItemIdValid(itemId)){
			UpdateKey key = new UpdateKey(uin,groupId,adpos,itemId);
			combinerKeys(key,value);		
		}
	}
}