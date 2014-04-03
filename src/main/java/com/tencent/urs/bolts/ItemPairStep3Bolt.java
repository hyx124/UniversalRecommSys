package com.tencent.urs.bolts;

import com.google.common.collect.ImmutableList;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.GroupPairInfo;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType;
import com.tencent.urs.protobuf.Recommend.UserPairInfo;

import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.HashSet;
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

public class ItemPairStep3Bolt  extends AbstractConfigUpdateBolt{
	private static final long serialVersionUID = -3578535683081183276L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private DataCache<Recommend.GroupPairInfo> groupPairCache;
	private UpdateCallBack putCallBack;
	private ConcurrentHashMap<String, HashMap<Long,Float>> combinerMap;

	private int dataExpireTime;
	private int cacheExpireTime;
	private int nsGroupPairTableId;
	
	private boolean debug;
	
	private static Logger logger = LoggerFactory
			.getLogger(ItemPairStep3Bolt.class);
	
	public ItemPairStep3Bolt(String config, ImmutableList<Output> outputField){
		super(config, outputField, Constants.config_stream);
	}
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		super.prepare(conf, context, collector);
		this.updateConfig(super.config);

		this.groupPairCache = new DataCache<Recommend.GroupPairInfo>(conf);
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.combinerMap = new ConcurrentHashMap<String,HashMap<Long,Float>>(1024);
				
		this.putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_send_interfaceID, 
				String.valueOf(this.nsGroupPairTableId));
		
		int combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5);
		setCombinerTime(combinerExpireTime);
	}	

	@Override
	public void updateConfig(XMLConfiguration config) {
		nsGroupPairTableId = config.getInt("group_pair_table",516);
		dataExpireTime = config.getInt("data_expiretime",7*24*3600);
		cacheExpireTime = config.getInt("cache_expiretime",3600);
		debug = config.getBoolean("debug",false);
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {
		// TODO Auto-generated method stub	
		try{
			String key = tuple.getStringByField("group_pair_key");
			Long timeId = tuple.getLongByField("time_id");
			Float weight = tuple.getFloatByField("weight");	
			
			combinerKeys(key,timeId,weight);

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
							HashMap<Long,Float> expireTimeValue  = combinerMap.remove(key);
							try{
								new GroupPairUpdateCallback(key,expireTimeValue).excute();
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
	
	private void combinerKeys(String key,Long timeId,Float weight) {
		synchronized(combinerMap){
			if(combinerMap.containsKey(key)){
				HashMap<Long,Float> oldValue = combinerMap.get(key);
				if(oldValue.containsKey(timeId)){
					oldValue.put(timeId, oldValue.get(timeId) + weight);
					combinerMap.put(key, oldValue);
				}
			}else{
				HashMap<Long,Float> newValue = new HashMap<Long,Float>();
				combinerMap.put(key, newValue);
			}
		}
	}	

	private class GroupPairUpdateCallback implements MutiClientCallBack{
		private final String putKey;
		private final HashMap<Long,Float> weightInfoMap;

		public GroupPairUpdateCallback(String key,HashMap<Long,Float> weightInfoMap) {
			this.putKey = key ; 
			this.weightInfoMap = weightInfoMap;
		}
		
		public void excute() {
			try {
				GroupPairInfo oldInfo = null;
				SoftReference<GroupPairInfo> sr = groupPairCache.get(putKey);
				if(sr != null){
					oldInfo = sr.get();
				}
				
				if(oldInfo != null){		
					next(oldInfo);
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsGroupPairTableId,putKey.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}			
				
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}

		private void save(String key,GroupPairInfo newInfo){	
		
			synchronized(groupPairCache){
				groupPairCache.set(key, new SoftReference<Recommend.GroupPairInfo>(newInfo), cacheExpireTime);
			}
			
			Future<Result<Void>> future = null;
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
				try {
					future = clientEntry.getClient().putAsync((short)nsGroupPairTableId, key.getBytes(),newInfo.toByteArray(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,key,newInfo.toByteArray(),putopt));
					
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
				break;
			}
		}
		
		@Override
		public void handle(Future<?> future, Object context) {
			// TODO Auto-generated method stub
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			GroupPairInfo oldInfo = null;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() != null){
					oldInfo	= GroupPairInfo.parseFrom(res.getResult());	
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);	
			}
			next(oldInfo);
		}
		
		public void next(GroupPairInfo oldWeightInfo){			
			GroupPairInfo.Builder newGroupInfoBuiler = GroupPairInfo.newBuilder();
			HashSet<Long> alreadyIn = new HashSet<Long>();
			Long now = System.currentTimeMillis()/1000;
			if(oldWeightInfo != null){
				for(GroupPairInfo.TimeSegment ts:oldWeightInfo.getTsegsList()){
					if(ts.getTimeId() < Utils.getDateByTime( now - dataExpireTime)){
						continue;
					}
					
					Float newCount = 0F;
					if(weightInfoMap.containsKey(ts.getTimeId())){
						newCount = ts.getCount() + weightInfoMap.get(ts.getTimeId());
						
						GroupPairInfo.TimeSegment.Builder tsBuilder = GroupPairInfo.TimeSegment.newBuilder();
						tsBuilder.setTimeId(ts.getTimeId()).setCount(newCount);
						newGroupInfoBuiler.addTsegs(tsBuilder.build());
					}else{
						newGroupInfoBuiler.addTsegs(ts);
					}
					
					alreadyIn.add(ts.getTimeId());
				}
			}
			
			for(Long key: weightInfoMap.keySet()){
				if(!alreadyIn.contains(key)){
					GroupPairInfo.TimeSegment.Builder tsBuilder = GroupPairInfo.TimeSegment.newBuilder();
					tsBuilder.setTimeId(key).setCount(weightInfoMap.get(key));

					newGroupInfoBuiler.addTsegs(0,tsBuilder.build());
					alreadyIn.add(key);
				}
			}
			save(putKey,newGroupInfoBuiler.build());
		}

		
	}

}