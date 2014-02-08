package com.tencent.urs.bolts;

import java.text.SimpleDateFormat;
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
import backtype.storm.tuple.Values;

import com.google.common.collect.ImmutableList;
import com.tencent.monitor.MonitorTools;

import com.tencent.streaming.commons.bolts.config.AbstractConfigUpdateBolt;
import com.tencent.streaming.commons.spouts.tdbank.Output;
import com.tencent.tde.client.Result;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.error.TairFlowLimit;
import com.tencent.tde.client.error.TairQueueOverflow;
import com.tencent.tde.client.error.TairRpcError;
import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.combine.GroupActionCombinerValue;
import com.tencent.urs.combine.UpdateKey;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.ActiveType;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.Utils;

public class ARCFBolt extends AbstractConfigUpdateBolt{
	/**
	 * 
	 */
	private static final long serialVersionUID = -7105767417026977180L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private ConcurrentHashMap<UpdateKey, GroupActionCombinerValue> combinerMap;
	private OutputCollector collector;
	private int nsGroupPairTableId;
	private int nsGroupCountTableId;
	private int nsDetailTableId;
	private int dataExpireTime;
	
	private static Logger logger = LoggerFactory.getLogger(ARCFBolt.class);

	@SuppressWarnings("rawtypes")
	public ARCFBolt(String config, ImmutableList<Output> outputField){
		super(config, outputField, Constants.config_stream);
	}
	
	@Override 
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		super.prepare(conf, context, collector);
		this.updateConfig(super.config);

		this.collector = collector;
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.combinerMap = new ConcurrentHashMap<UpdateKey,GroupActionCombinerValue>(1024);

		int combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5);
		setCombinerTime(combinerExpireTime);
	} 
	
	@Override
	public void updateConfig(XMLConfiguration config) {
		nsGroupCountTableId = config.getInt("group_count_table",304);
		nsGroupPairTableId = config.getInt("group_pair_table",306);
		nsDetailTableId = config.getInt("dependent_table",302);
		dataExpireTime = config.getInt("data_expiretime",1*24*3600);
		//cacheExpireTime = config.getInt("cache_expiretime",3600);
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {
		String bid = tuple.getStringByField("bid");
		String qq = tuple.getStringByField("qq");
		String groupId = tuple.getStringByField("group_id");
		String itemId = tuple.getStringByField("item_id");
		String adpos = tuple.getStringByField("adpos");
		
		//String actionType = tuple.getStringByField("action_type");
		String actionTime = tuple.getStringByField("action_time");
		
		//ActiveType actType = Utils.getActionTypeByString(actionType);
		
		if(!Utils.isBidValid(bid) || !Utils.isQNumValid(qq) || !Utils.isGroupIdVaild(groupId) || !Utils.isItemIdValid(itemId)){
			return;
		}
		
		//GroupActionCombinerValue value = new GroupActionCombinerValue(actType,Long.valueOf(actionTime));
		UpdateKey key = new UpdateKey(bid,Long.valueOf(qq),Integer.valueOf(groupId),adpos,itemId);
		//combinerKeys(key,value);	
		
		Double weight = (double) (1000-Long.valueOf(actionTime)%100);
		doEmit("C1001",bid,itemId,groupId,actionTime,weight);
		
		doEmit("A1001",bid,itemId,"0",actionTime,weight);
		doEmit("B1001",bid,itemId,"0",actionTime,weight);
		
	}
	
	private void  doEmit(String bid,String algName, String itemId, String groupId,String actionTime,double weight){
		String testKey_c = "1#"+itemId+"#1#"+algName+"#"+groupId;
		
		Long now = System.currentTimeMillis()/1000L;
		String otheritemId = String.valueOf(now%30+1);
		Values values_c = new Values(bid,testKey_c,otheritemId,weight,algName);
		this.collector.emit("computer_result",values_c);

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
							GroupActionCombinerValue value = combinerMap.remove(key);
							try{
								new GetPairsListCallBack(key,value).excute();
							}catch(Exception e){
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
		combinerMap.put(key, value);
	}	
	
	private class GetPairsCountCallBack implements MutiClientCallBack{
		private final UpdateKey key;
		private final Integer itemCount1;
		private final Integer itemCount2;
		private final String otherItem;
		private String getKey;
		
		public GetPairsCountCallBack(UpdateKey key,String otherItem,Integer itemCount1,Integer itemCount2) {
			this.key = key ; 
			this.otherItem = otherItem;
			this.itemCount1 = itemCount1;
			this.itemCount2 = itemCount2;
			this.getKey = key.getGroupPairKey(otherItem);
		}

		public void excute() {
			try {		
				ClientAttr clientEntry = mtClientList.get(0);		
				TairOption opt = new TairOption(clientEntry.getTimeout());
				Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsGroupPairTableId,getKey.toString().getBytes(),opt);
				clientEntry.getClient().notifyFuture(future, this,clientEntry);			
			} catch (TairQueueOverflow e) {
				logger.error(e.toString());
			} catch (TairRpcError e) {
				logger.error(e.toString());
			} catch (TairFlowLimit e) {
				logger.error(e.toString());
			}
		}

		public Double computeARWeight(Integer itemCount1, Integer itemCount2,
				Integer pairCount) {	
			Double simAR = (double) (pairCount/itemCount1);
			return simAR;
		}
		
		public Double computeCFWeight(Integer itemCount1, Integer itemCount2,
				Integer pairCount) {	
			//CF
			Double simCF = (double) (pairCount/(Math.sqrt(itemCount1) * Math.sqrt(itemCount2)));
			return simCF;
		}
		
		@Override
		public void handle(Future<?> future, Object context) {			
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			Integer pairCount = 0;
			try {
				Result<byte[]> result = afuture.get();	
				if(result.isSuccess() && result.getResult()!=null){
					pairCount = Integer.valueOf(new String(result.getResult()));
				}
			} catch (Exception e) {
				
			}
			
			Double arWeight = computeARWeight(itemCount1,itemCount2,pairCount);
			Double cfWeight = computeCFWeight(itemCount1,itemCount2,pairCount);
			doEmit(key.getBid(),key.getItemId(),key.getAdpos(),otherItem,Constants.alg_cf,String.valueOf(key.getGroupId()),cfWeight);
			doEmit(key.getBid(),key.getItemId(),key.getAdpos(),otherItem,Constants.alg_ar,String.valueOf(key.getGroupId()),arWeight);	
		}
		
		private void doEmit(String bid,String itemId,String adpos,String otherItem,  String algName, String groupId, double weight){
			String resultKey = Utils.getAlgKey(bid,itemId, adpos, algName, groupId);		
			Values values = new Values(resultKey,adpos,otherItem,weight,algName);
			
			
			String resultKey2 = Utils.getAlgKey(bid,otherItem, adpos, algName, groupId);		
			Values values2 = new Values(resultKey2,adpos,itemId,weight,algName);
			
			collector.emit("computer_result",values);
			collector.emit("computer_result",values2);
			
			if(!groupId.equals("0")){
				
				String resultKey3 = Utils.getAlgKey(bid,itemId, adpos, algName, "0");		
				Values values3 = new Values(resultKey3,adpos,otherItem,weight,algName);
				
				
				String resultKey4 = Utils.getAlgKey(bid,otherItem, adpos, algName, "0");		
				Values values4 = new Values(resultKey4,adpos,itemId,weight,algName);
				collector.emit("computer_result",values3);
				collector.emit("computer_result",values4);
			}
			
		}
	}
	
	private class GetItemCountCallBack implements MutiClientCallBack{
		private final UpdateKey key;
		private String otherItem;
		private Integer itemCount;
		private Integer step;

		public GetItemCountCallBack(UpdateKey key,String otherItem,Integer itemCount,Integer step) {
			this.key = key ; 
			this.otherItem = otherItem;
			this.itemCount = itemCount;
			this.step = step;
		}

		public void excute() {
			try {
				String getKey = "";
				if(step == 1){
					getKey = key.getGroupCountKey();
				}else if(step == 2){
					getKey = key.getOtherGroupCountKey(otherItem);
				}else{
					return;
				}
								
				
				ClientAttr clientEntry = mtClientList.get(0);		
				TairOption opt = new TairOption(clientEntry.getTimeout());
				Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsGroupCountTableId,getKey.getBytes(),opt);
				clientEntry.getClient().notifyFuture(future, this,clientEntry);		
			} catch (TairQueueOverflow e) {
				//log.error(e.toString());
			} catch (TairRpcError e) {
				//log.error(e.toString());
			} catch (TairFlowLimit e) {
				//log.error(e.toString());
			}
		}

		@Override
		public void handle(Future<?> future, Object context) {			
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			String count = "0";
			try {
				if(afuture.get().isSuccess() && afuture.get().getResult()!=null){
					count = new String(afuture.get().getResult());
				}
			} catch (Exception e) {
				
			}
			
			if(step == 1){
				new GetItemCountCallBack(key,otherItem,Integer.valueOf(count),2).excute();
			}else if(step == 2){
				new GetPairsCountCallBack(key,otherItem,itemCount,Integer.valueOf(count)).excute();
			}
			
			
		}
	}
	
	private class GetPairsListCallBack implements MutiClientCallBack{
		private final UpdateKey key;
		private GroupActionCombinerValue value;
		public GetPairsListCallBack(UpdateKey key,GroupActionCombinerValue value) {
			this.key = key ; 
			this.value = value;
			//this.itemCount = itemCount;
		}

		public void excute() {
			try {
				String checkKey = key.getDetailKey();
				
				ClientAttr clientEntry = mtClientList.get(0);		
				TairOption opt = new TairOption(clientEntry.getTimeout());
				Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsDetailTableId,checkKey.getBytes(),opt);
				clientEntry.getClient().notifyFuture(future, this,clientEntry);	

			} catch (TairQueueOverflow e) {
				//log.error(e.toString());
			} catch (TairRpcError e) {
				//log.error(e.toString());
			} catch (TairFlowLimit e) {
				//log.error(e.toString());
			}
		}
		
		private Long getWinIdByTime(Long time){	
			String expireId = new SimpleDateFormat("yyyyMMdd").format(time*1000L);
			return Long.valueOf(expireId);
		}
		
		private HashSet<String> getPairItems(UserActiveDetail oldValueHeap, String itemId){
			HashSet<String>  itemSet = new HashSet<String>();		
			
			for(Recommend.UserActiveDetail.TimeSegment tsegs:oldValueHeap.getTsegsList()){
				if(tsegs.getTimeId() > getWinIdByTime(value.getTime() - dataExpireTime)){
					for(Recommend.UserActiveDetail.TimeSegment.ItemInfo item: tsegs.getItemsList()){
						if(!item.getItem().equals(key.getItemId())){
							itemSet.add(item.getItem());
						}
					}
				}
			}
			return itemSet;
		}

		@Override
		public void handle(Future<?> future, Object context) {			
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			try {
				Result<byte[]> result = afuture.get();	
				if(result.isSuccess() && result.getResult()!=null){
					UserActiveDetail oldValueHeap = Recommend.UserActiveDetail.parseFrom(result.getResult());
					HashSet<String> itemSet = getPairItems(oldValueHeap,key.getItemId());
					for(String otherItem:itemSet){
						new GetItemCountCallBack(key,otherItem, 0,1).excute();
					}					
				}
			} catch (Exception e) {
				
			}
			
		}
	}


}