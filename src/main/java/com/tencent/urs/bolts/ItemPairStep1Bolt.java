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

public class ItemPairStep1Bolt  extends AbstractConfigUpdateBolt{
	private static final long serialVersionUID = -3578535683081183276L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private ConcurrentHashMap<UpdateKey, GroupActionCombinerValue> combinerMap;

	private int nsDetailTableId;
	private int dataExpireTime;
	
	private boolean debug;
	private OutputCollector collector;
	
	private static Logger logger = LoggerFactory
			.getLogger(ItemPairStep1Bolt.class);
	
	public ItemPairStep1Bolt(String config, ImmutableList<Output> outputField){
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

		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.combinerMap = new ConcurrentHashMap<UpdateKey,GroupActionCombinerValue>(1024);
				
		this.collector = collector;
		int combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5);
		setCombinerTime(combinerExpireTime);
	}	

	@Override
	public void updateConfig(XMLConfiguration config) {
		nsDetailTableId = config.getInt("dependent_table",512);
		dataExpireTime = config.getInt("data_expiretime",7*24*3600);
		debug = config.getBoolean("debug",false);
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {
		// TODO Auto-generated method stub	
		try{
			String bid = tuple.getStringByField("bid");
			String qq = tuple.getStringByField("qq");
			String groupId = tuple.getStringByField("group_id");
			String adpos = Constants.DEFAULT_ADPOS;
			String itemId = tuple.getStringByField("item_id");
			
			String actionType = tuple.getStringByField("action_type");
			String actionTime = tuple.getStringByField("action_time");
						
			if(!Utils.isBidValid(bid) || !Utils.isQNumValid(qq) 
					|| !Utils.isGroupIdVaild(groupId) || !Utils.isItemIdValid(itemId)){
				return;
			}

			GroupActionCombinerValue value = 
					new GroupActionCombinerValue(Integer.valueOf(actionType),Long.valueOf(actionTime));
			UpdateKey key = new UpdateKey(bid,Long.valueOf(qq),Integer.valueOf(groupId),adpos,itemId);
			combinerKeys(key,value);

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
							GroupActionCombinerValue expireTimeValue  = combinerMap.remove(key);
							try{
								new ActionDetailCallBack(key,expireTimeValue).excute();
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
	
	private Float getWeightByType(Integer actionType){
		return Utils.getActionWeight(actionType);
	}
	
	private class ActionDetailCallBack implements MutiClientCallBack{
		private final UpdateKey key;
		private final String checkKey;
		private final GroupActionCombinerValue values;

		public ActionDetailCallBack(UpdateKey key, GroupActionCombinerValue values){
			this.key = key ; 
			this.values = values;		
			this.checkKey =  key.getDetailKey();
		}

		private void next(HashMap<String,MidInfo> weightMap){
			if(weightMap != null){
				if(debug && key.getUin() == 389687043L){
					for(String sendKey: weightMap.keySet()){
						logger.info("step1,end to step2,uin="+key.getUin()+", key="+sendKey+",weight="+weightMap.get(sendKey).getWeight()
								+",timeId="+weightMap.get(sendKey).getTimeId());
					}
				}
				
				List<Map.Entry<String, MidInfo>> sortList =
					    new ArrayList<Map.Entry<String, MidInfo>>(weightMap.entrySet());
				
				Collections.sort(sortList, new Comparator<Map.Entry<String, MidInfo>>() {   
					@Override
					public int compare(Entry<String, MidInfo> arg0,
							Entry<String, MidInfo> arg1) {
						 return (int)(arg1.getValue().getTimeId() - arg0.getValue().getTimeId());
					}
				}); 
				
				StringBuffer mergeItems = new StringBuffer();		
				int count = 0;
				for(Entry<String, MidInfo> sortItem: sortList){
					if(count >= 50){
						break;
					}
					
					mergeItems.append(sortItem.getKey()).append(",")
								.append(sortItem.getValue().getTimeId()).append(",")
								.append(sortItem.getValue().getWeight()).append(";");
					
					count ++;
				}
				
				Values outputValues = new Values();		
				outputValues.add(key.getBid());
				outputValues.add(String.valueOf(key.getUin()));
				outputValues.add(String.valueOf(key.getGroupId()));
				outputValues.add(mergeItems.toString());
				synchronized(collector){
					collector.emit(Constants.user_pair_stream,outputValues);
				}
			}
		}
		
		public void excute() {
			try {			
				ClientAttr clientEntry = mtClientList.get(0);		
				TairOption opt = new TairOption(clientEntry.getTimeout());
				Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsDetailTableId,checkKey.getBytes(),opt);
				clientEntry.getClient().notifyFuture(future, this,clientEntry);	
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}

		@Override
		public void handle(Future<?> future, Object context) {			
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult()!=null){
					UserActiveDetail oldValueHeap = UserActiveDetail.parseFrom(res.getResult());
					HashMap<String,MidInfo> weightMap = getPairItems(oldValueHeap , values);
					next(weightMap);
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}	
		}
		
		private HashMap<String,MidInfo> getPairItems(UserActiveDetail oldValueHeap, GroupActionCombinerValue values ){
			HashMap<String,MidInfo> weightMap = new HashMap<String,MidInfo>();
			
			Float thisItemWeight = getWeightByType(values.getType());
			if(thisItemWeight == null ){
				return null;
			}
			
			MidInfo newValueInfo = new MidInfo(Utils.getDateByTime(values.getTime()),thisItemWeight);
			String doubleKey = Utils.getItemPairKey(key.getItemId(),key.getItemId());
			weightMap.put(doubleKey,newValueInfo);
			
			for(TimeSegment ts:oldValueHeap.getTsegsList()){
				if(ts.getTimeId() < Utils.getDateByTime(values.getTime() - dataExpireTime)){
					continue;
				}
			
				for(ItemInfo item:ts.getItemsList()){			
					String itemPairKey = Utils.getItemPairKey(key.getItemId(),item.getItem());
					for(ActType act: item.getActsList()){	
						Float actWeight = getWeightByType(act.getActType());
						if(weightMap.containsKey(itemPairKey)){	
							if(weightMap.get(itemPairKey).getWeight() < actWeight){
								MidInfo midInfo = new MidInfo(ts.getTimeId(),actWeight);
								weightMap.put(itemPairKey, midInfo);
							}		
						}else{
							MidInfo midInfo = new MidInfo(ts.getTimeId(),actWeight);
							weightMap.put(itemPairKey, midInfo);
						}
					}			
				}
			}
			
			MidInfo doubleValue = weightMap.remove(doubleKey);
			for(String pairKey: weightMap.keySet()){
				if(!pairKey.equals(doubleKey)){
					Float minWeight =  Math.min(weightMap.get(pairKey).getWeight(), doubleValue.getWeight());
					Long minTimeId =  Math.min(weightMap.get(pairKey).getTimeId(), doubleValue.getTimeId());
					MidInfo minWeightInfo = new MidInfo(minTimeId,minWeight);
					weightMap.put(pairKey, minWeightInfo);
				}
			}
			return weightMap;
		}
		
	}
}