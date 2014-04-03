package com.tencent.urs.bolts;

import com.google.common.collect.ImmutableList;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.GroupPairInfo;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail;
import com.tencent.urs.protobuf.Recommend.UserActiveHistory;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType;
import com.tencent.urs.protobuf.Recommend.UserActiveHistory.ActiveRecord;
import com.tencent.urs.protobuf.Recommend.UserPairInfo;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

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

public class ItemPairStep2Bolt  extends AbstractConfigUpdateBolt{
	private static final long serialVersionUID = -3578535683081183276L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private DataCache<Recommend.UserPairInfo> userPairCache;
	private UpdateCallBack putCallBack;
	private ConcurrentHashMap<UpdateKey, HashMap<String,MidInfo>> combinerMap;

	private int dataExpireTime;
	private int cacheExpireTime;
	private int nsUserPairTableId;
	
	private boolean debug;
	private OutputCollector collector;
	
	private static Logger logger = LoggerFactory
			.getLogger(ItemPairStep2Bolt.class);
	
	public ItemPairStep2Bolt(String config, ImmutableList<Output> outputField){
		super(config, outputField, Constants.config_stream);
	}
	
	public class MidInfo {
		private Long timeId;
		private Float weight;
		
		MidInfo(Long timeId,Float weight){
			this.timeId = timeId;
			this.weight = weight;
		}
		
		public Long getTimeId(){
			return this.timeId;
		}
		
		public Float getWeight(){
			return this.weight;
		}
	}
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		super.prepare(conf, context, collector);
		this.updateConfig(super.config);

		this.userPairCache = new DataCache<Recommend.UserPairInfo>(conf);
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.combinerMap = new ConcurrentHashMap<UpdateKey,HashMap<String,MidInfo>>(1024);
				
		this.putCallBack = new UpdateCallBack(mt, Constants.systemID, 
				Constants.tde_back_interfaceID,
				String.valueOf(this.nsUserPairTableId));
		this.collector = collector;
		int combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5);
		setCombinerTime(combinerExpireTime);
	}	

	@Override
	public void updateConfig(XMLConfiguration config) {
		nsUserPairTableId = config.getInt("user_pair_table",515);
		dataExpireTime = config.getInt("data_expiretime",7*24*3600);
		cacheExpireTime = config.getInt("cache_expiretime",3600);
		debug = config.getBoolean("debug",false);
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {
		// TODO Auto-generated method stub	
		try{
			String bid = tuple.getStringByField("bid");
			String qq = tuple.getStringByField("qq");
			String groupId = tuple.getStringByField("group_id");
			String mergeItems = tuple.getStringByField("merge_items");
			
			String items[] =  mergeItems.split(";",-1);
			HashMap<String, MidInfo> itemMap = new HashMap<String, MidInfo>();
			for(String each: items){
				String[] infos = each.split(",",-1);
				if(infos.length == 3){
					MidInfo midInfo = new MidInfo(Long.valueOf(infos[1]),Float.valueOf(infos[2]));
					itemMap.put(infos[0], midInfo);
				}
			}
			
			logger.info("add to combiner");
			UpdateKey key = new UpdateKey(bid,Long.valueOf(qq),Integer.valueOf(groupId),"1","0");
			combinerKeys(key,itemMap);
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
						Set<UpdateKey> keySet = combinerMap.keySet();
						for (UpdateKey key : keySet) {
							HashMap<String, MidInfo> expireTimeValue  = combinerMap.remove(key);
							try{
								new UserPairUpdateCallBack(key,expireTimeValue).excute();
							}catch(Exception e){
								logger.error(e.getMessage(), e);
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
	
	private void combinerKeys(UpdateKey userKey,HashMap<String, MidInfo> itemMap) {		
		synchronized(combinerMap){
			if(combinerMap.contains(userKey)){
				HashMap<String, MidInfo> otherItemPairs = combinerMap.get(userKey);
				otherItemPairs.putAll(itemMap);
				combinerMap.put(userKey, otherItemPairs);
			}else{
				combinerMap.put(userKey, itemMap);
			}
		}
	}	

	private class UserPairUpdateCallBack implements MutiClientCallBack{
		private HashMap<String,MidInfo> weightMap;
		private UpdateKey key;
		private String userPairKey;

		public UserPairUpdateCallBack(UpdateKey key, HashMap<String,MidInfo> weightMap) {
			this.key = key ; 
			this.weightMap = weightMap;			
			this.userPairKey = key.getUserPairKey();
		}
		
		public void excute() {
			try {
				UserPairInfo oldInfo = null;
				SoftReference<UserPairInfo> sr = userPairCache.get(userPairKey);	
				if(sr != null){
					oldInfo = sr.get();
				}
				
				if(oldInfo != null){
					logger.info("get from cache");
					next(oldInfo);
				}else{
					logger.info("get from tde");
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsUserPairTableId,userPairKey.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}			
				
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
		
		private void newNext(UserPairInfo oldWeightInfo){
			
		}
		
		private void next(UserPairInfo oldWeightInfo){
			logger.info("deal next");
			LinkedList<UserPairInfo.ItemPairs> newUserPairInfoList = new LinkedList<UserPairInfo.ItemPairs>();
			
			HashMap<String,HashMap<Long,MidInfo>> changeMap = new HashMap<String,HashMap<Long,MidInfo>>();
			
			HashSet<String> alreadyIn = new HashSet<String>();
			Long now = System.currentTimeMillis()/1000;
			if(oldWeightInfo != null){
				for(UserPairInfo.ItemPairs ipairs:oldWeightInfo.getIpairsList()){
					
					if(this.weightMap.containsKey(ipairs.getItemPair())){
						Long lastTimeId = ipairs.getLastTimeId();
						float lastWeight = ipairs.getLastCount();
						Long thisTimeId = weightMap.get(ipairs.getItemPair()).getTimeId();
						float thisWeight = weightMap.get(ipairs.getItemPair()).getWeight();
						
						if(debug && key.getUin() == 389687043L){
							logger.info("step2,add new info,key="+ipairs.getItemPair()
									+",lastTimeId="+lastTimeId+",lastWeight="+lastWeight
									+",thisTimeId="+thisTimeId+",thisWeight="+thisWeight);
						}
						
						HashMap<Long,MidInfo> midMap = new HashMap<Long,MidInfo>();
						
						if(lastTimeId ==  thisTimeId ){
							float changeWeight =  thisWeight - lastWeight;
							if(changeWeight != 0){
								MidInfo changeInfo = new MidInfo(thisTimeId, thisWeight - lastWeight);
								midMap.put(thisTimeId, changeInfo);
							}
						}else if(lastTimeId !=  thisTimeId){
							MidInfo changeLastInfo = new MidInfo(lastTimeId, 0 - lastWeight);
							midMap.put(lastTimeId, changeLastInfo);
							
							MidInfo changeThisInfo = new MidInfo(thisTimeId, thisWeight - 0);
							midMap.put(thisTimeId, changeThisInfo);
							
							
						}	
						changeMap.put(ipairs.getItemPair(), midMap);
						
						UserPairInfo.ItemPairs.Builder ipairsBuilder = UserPairInfo.ItemPairs.newBuilder();
						ipairsBuilder.setItemPair(ipairs.getItemPair()).setLastTimeId(thisTimeId).setLastCount(thisWeight);
						newUserPairInfoList.add(ipairsBuilder.build());
						
						alreadyIn.add(ipairs.getItemPair());
					}else if(ipairs.getLastTimeId() >= Utils.getDateByTime(now -dataExpireTime) ){
						newUserPairInfoList.add(ipairs);
					}		
				}
			}

			for(String leftKey : this.weightMap.keySet()){
				if(alreadyIn.contains(leftKey)){
					continue;
				}
			
				Long thisTimeId = weightMap.get(leftKey).getTimeId();
				float thisWeight = weightMap.get(leftKey).getWeight();
				
				MidInfo leftMidInfo = new MidInfo(thisTimeId, thisWeight);
				
				HashMap<Long,MidInfo> midMap = new HashMap<Long,MidInfo>();
				midMap.put(thisTimeId, leftMidInfo);
				changeMap.put(leftKey, midMap);
				
				
				if(debug && key.getUin() == 389687043L){
					logger.info("step2,not find old info, add new,key="+leftKey
							+",thisTimeId="+thisTimeId+",thisWeight="+thisWeight);
				}
				
				UserPairInfo.ItemPairs.Builder ipairsBuilder = UserPairInfo.ItemPairs.newBuilder();
				ipairsBuilder.setItemPair(leftKey).setLastTimeId(thisTimeId).setLastCount(thisWeight);
				newUserPairInfoList.add(ipairsBuilder.build());
				
				alreadyIn.add(leftKey);
			}

			logger.info("sort");
			Collections.sort(newUserPairInfoList, new Comparator<UserPairInfo.ItemPairs>() {   
				@Override
				public int compare(UserPairInfo.ItemPairs arg0,
						UserPairInfo.ItemPairs arg1) {
					 return (int)(arg1.getLastTimeId() - arg0.getLastTimeId());
				}
			}); 
			
			UserPairInfo.Builder newUserInfoBuiler = UserPairInfo.newBuilder();
			for(UserPairInfo.ItemPairs ip: newUserPairInfoList){
				if(newUserInfoBuiler.getIpairsCount() >= 500){
					break;
				}
				newUserInfoBuiler.addIpairs(ip);
			}
			save(userPairKey,newUserInfoBuiler.build());	
			
			logger.info("send to step3");
			for(String itemPairKey: changeMap.keySet()){
				String groupKey = Utils.spliceStringBySymbol("#", String.valueOf(key.getBid()),
						itemPairKey,String.valueOf(key.getGroupId()));
				
				String noGroupKey = Utils.spliceStringBySymbol("#", String.valueOf(key.getBid()),
						itemPairKey,"0");
				
				HashMap<Long, MidInfo> midMap = changeMap.get(itemPairKey);
					
				for(Long timeId: midMap.keySet()){
					Float weight = midMap.get(timeId).getWeight();
					if(weight != null && weight != 0){
						Values outputValues = new Values();		
						outputValues.add(groupKey);
						outputValues.add(timeId);
						outputValues.add(weight);	
						
						synchronized(collector){
							collector.emit(Constants.group_pair_stream,outputValues);
						}
						
						logger.info("send to step3,success");
						if(key.getGroupId() != 0){
							Values noGroupOutputValues = new Values();		
							noGroupOutputValues.add(noGroupKey);
							noGroupOutputValues.add(timeId);
							noGroupOutputValues.add(weight);	
							synchronized(collector){
								collector.emit(Constants.group_pair_stream,noGroupOutputValues);
							}
						}
					}	
				}
			}
		}
		
		private void save(String userPairKey,UserPairInfo newInfo){
			Future<Result<Void>> future = null;
			synchronized(userPairCache){
				userPairCache.set(userPairKey, new SoftReference<UserPairInfo>(newInfo), cacheExpireTime);
			}
			
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
				try {
					future = clientEntry.getClient().putAsync((short)nsUserPairTableId, userPairKey.getBytes(),newInfo.toByteArray(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,userPairKey,newInfo.toByteArray(),putopt));
					
					/*
					if(mt!=null){
						MonitorEntry mEntryPut = new MonitorEntry(Constants.SUCCESSCODE,Constants.SUCCESSCODE);
						mEntryPut.addExtField("TDW_IDC", clientEntry.getGroupname());
						mEntryPut.addExtField("tbl_name", "FIFO1");
						mt.addCountEntry(Constants.systemID, Constants.tde_put_interfaceID, mEntryPut, 1);
					}*/
					break;
				} catch (Exception e){
					logger.error(e.getMessage(), e);
				}
				break;
			}
		}
			
		@Override
		public void handle(Future<?> future, Object arg1) {
			// TODO Auto-generated method stub
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			UserPairInfo oldInfo = null;
			logger.info("back from tde");
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult()!=null){
					oldInfo = UserPairInfo.parseFrom(res.getResult());
				}	
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
			next(oldInfo);
		}
		
	}
}