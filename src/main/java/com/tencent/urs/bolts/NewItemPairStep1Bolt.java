package com.tencent.urs.bolts;

import com.google.common.collect.ImmutableList;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType;
import com.tencent.urs.protobuf.Recommend.UserPairInfo;

import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
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
import backtype.storm.tuple.Values;

import com.tencent.monitor.MonitorTools;

import com.tencent.streaming.commons.bolts.config.AbstractConfigUpdateBolt;
import com.tencent.streaming.commons.spouts.tdbank.Output;
import com.tencent.tde.client.Result;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBackContext;
import com.tencent.urs.combine.ActionCombinerValue;
import com.tencent.urs.combine.GroupActionCombinerValue;
import com.tencent.urs.combine.UpdateKey;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.Utils;

public class NewItemPairStep1Bolt  extends AbstractConfigUpdateBolt{
	private static final long serialVersionUID = -3578535683081183276L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private HashMap<UpdateKey, GroupActionCombinerValue> liveCombinerMap;

	private int nsDetailTableId;
	private int nsUserPairTableId;
	private int linkedTime;
	private OutputCollector collector;

	private boolean debug;
	
	private static Logger logger = LoggerFactory
			.getLogger(NewItemPairStep1Bolt.class);
	
	public NewItemPairStep1Bolt(String config, ImmutableList<Output> outputField){
		super(config, outputField, Constants.config_stream);
	}
	
	public class MidInfo {
		private long timeId;
		private float weight;
		
		MidInfo(long timeId,float weight){
			this.timeId = timeId;
			this.weight = weight;
		}
		
		public long getTimeId(){
			return this.timeId;
		}
		
		public float getWeight(){
			return this.weight;
		}

		public void setWeigth(Float actWeight) {
			this.weight = actWeight;
		}
		
		public void setTime(Long timeId) {
			this.timeId = timeId;
		}

	}
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		super.prepare(conf, context, collector);
		this.updateConfig(super.config);

		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.liveCombinerMap = new HashMap<UpdateKey,GroupActionCombinerValue>(1024);
		this.collector = collector;		
		int combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5);
		setCombinerTime(combinerExpireTime);
	}	

	@Override
	public void updateConfig(XMLConfiguration config) {
		nsUserPairTableId = config.getInt("user_pair_table",515);
		nsDetailTableId = config.getInt("dependent_table",512);
		linkedTime = config.getInt("linked_time",24*3600);
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
						
						HashMap<UpdateKey,GroupActionCombinerValue> deadCombinerMap = null;
						synchronized (liveCombinerMap) {
							deadCombinerMap = liveCombinerMap;
							liveCombinerMap = new HashMap<UpdateKey,GroupActionCombinerValue>(1024);
						}
						
						Set<UpdateKey> keySet = deadCombinerMap.keySet();
						for (UpdateKey key : keySet) {
							GroupActionCombinerValue expireTimeValue  = deadCombinerMap.get(key);
							try{
								new ActionDetailCallBack(key,expireTimeValue).excute();
							}catch(Exception e){
								logger.error(e.getMessage(), e);
							}
						}
						deadCombinerMap.clear();
						deadCombinerMap = null;
					}
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
				}
			}
		}).start();
	}
	
	private void combinerKeys(UpdateKey key,GroupActionCombinerValue value) {
		synchronized(liveCombinerMap){
			if(liveCombinerMap.containsKey(key)){
				GroupActionCombinerValue oldValue = liveCombinerMap.get(key);
				oldValue.incrument(value);
				liveCombinerMap.put(key, oldValue);
			}else{
				liveCombinerMap.put(key, value);
			}
		}
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
					sendToNextBolt(weightMap);
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}	
		}
		
		private void sendToNextBolt(HashMap<String,MidInfo> changeMap){

			for(String itemPairKey: changeMap.keySet()){
				String groupKey = Utils.spliceStringBySymbol("#", String.valueOf(key.getBid()),
						itemPairKey,String.valueOf(key.getGroupId()));
				
				String noGroupKey = Utils.spliceStringBySymbol("#", String.valueOf(key.getBid()),
						itemPairKey,"0");
				
				MidInfo midInfo = changeMap.get(itemPairKey);
	
				if(midInfo.getWeight() != 0){						
					synchronized(collector){
						collector.emit(Constants.group_pair_stream,
								new Values(groupKey,midInfo.getTimeId(),midInfo.getWeight()));
					}
						
					if(key.getGroupId() != 0){						
						synchronized(collector){
							collector.emit(Constants.group_pair_stream,
									new Values(noGroupKey,midInfo.getTimeId(),midInfo.getWeight()));
						}
					}
				}	
			}
		}
		
		private ActType getMaxActFromType(List<ActType> actList){
			ActType maxIndex = null;
			float maxWeight = 0F;
			for(ActType act: actList){	
				Float actWeight = Utils.getActionWeight(act.getActType());
				
				if(actWeight > maxWeight){
					maxIndex = act;
				}
			}
			return maxIndex;
		}
		
		private HashMap<String,MidInfo> getPairItems(UserActiveDetail oldValueHeap, GroupActionCombinerValue values ){
			HashMap<String,MidInfo> weightMap = new HashMap<String,MidInfo>();
			
			Float thisItemWeight = Utils.getActionWeight(values.getType());
			if(thisItemWeight == null ){
				return null;
			}
			
			MidInfo newValueInfo = new MidInfo(Utils.getDateByTime(values.getTime()),thisItemWeight);
			String doubleKey = Utils.getItemPairKey(key.getItemId(),key.getItemId());
			weightMap.put(doubleKey,newValueInfo);
			
			for(TimeSegment ts:oldValueHeap.getTsegsList()){	
				if(ts.getTimeId() < Utils.getDateByTime(values.getTime() - linkedTime)){
					continue;
				}
				
				for(ItemInfo item:ts.getItemsList()){	
					
					ActType maxAct = getMaxActFromType(item.getActsList());
					float maxWeight = Utils.getActionWeight(maxAct.getActType());

					if(key.getItemId().equals((item.getItem()))){
						if(thisItemWeight == maxWeight &&
								maxAct.getCount() > 1){
							return null;
						}else if(thisItemWeight < maxWeight ){
							return null;
						}else{
							MidInfo maxValueInfo = new MidInfo(ts.getTimeId(),maxWeight);
							weightMap.put(doubleKey, maxValueInfo);
						}		
					}else if(maxAct.getLastUpdateTime()  > values.getTime() - linkedTime){
						String itemPairKey = Utils.getItemPairKey(key.getItemId(),item.getItem());
						if(maxWeight == 0){
							continue;
						}
						if(weightMap.containsKey(itemPairKey)){	
							if(weightMap.get(itemPairKey).getWeight() < maxWeight){
								MidInfo midInfo = new MidInfo(ts.getTimeId(),maxWeight);
								weightMap.put(itemPairKey, midInfo);
							}		
						}else{
							MidInfo midInfo = new MidInfo(ts.getTimeId(),maxWeight);
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
	
	public static void main(String[] args){
	}
	
}