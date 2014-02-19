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
		try{
			String bid = tuple.getStringByField("bid");
			String qq = tuple.getStringByField("qq");
			String groupId = tuple.getStringByField("group_id");
			String itemId = tuple.getStringByField("item_id");
			String adpos = tuple.getStringByField("adpos");
			
			String actionType = tuple.getStringByField("action_type");
			String actionTime = tuple.getStringByField("action_time");
			
			ActiveType actType = Utils.getActionTypeByString(actionType);
			
			if(!Utils.isBidValid(bid) || !Utils.isQNumValid(qq) || !Utils.isGroupIdVaild(groupId) || !Utils.isItemIdValid(itemId)){
				return;
			}
			
			GroupActionCombinerValue value = new GroupActionCombinerValue(actType,Long.valueOf(actionTime));
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
							GroupActionCombinerValue value = combinerMap.remove(key);
							try{
								new GetPairsListCallBack(key,value).excute();
							}catch(Exception e){
								logger.error(e.getMessage(), e);
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
		combinerMap.put(key, value);
	}	
	
	private class GetPairsCountCallBack implements MutiClientCallBack{
		private final UpdateKey key;
		private Float itemCount1;
		private Float itemCount2;
		private final String otherItem;
		private String getKey;
		
		public GetPairsCountCallBack(UpdateKey key,String otherItem,Float itemCount1,Float itemCount2) {
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
			} catch (Exception e){
				logger.error(e.getMessage(), e);
			}
		}

		public Double computeARWeight(Float itemCount1, Float itemCount2,
				Float pairCount) {	
			Double simAR = (double) (pairCount/itemCount1);
			return simAR;
		}
		
		public Double computeCFWeight(Float itemCount1, Float itemCount2,
				Float pairCount) {	
			//CF
			Double simCF = (double) (pairCount/(Math.sqrt(itemCount1) * Math.sqrt(itemCount2)));
			return simCF;
		}
		
		@Override
		public void handle(Future<?> future, Object context) {			
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			Float pairCount = 0F;
			logger.info("enter step3");
			try {
				Result<byte[]> result = afuture.get();	
				if(result.isSuccess() && result.getResult()!=null){
					pairCount = Float.parseFloat(new String(result.getResult()));
				}
			} catch (Exception e){
				logger.error(e.getMessage(), e);
			}
			
			//logger.info("item1="+key.getItemId()+",item2="+otherItem+"itemcount1="+itemCount1+",itemcount2="+itemCount2+",paircount="+pairCount);
			
			Double cf12Weight = computeCFWeight(itemCount1,itemCount2,pairCount);
			Double cf21Weight = computeCFWeight(itemCount2,itemCount1,pairCount);
			Double ar12Weight = computeARWeight(itemCount1,itemCount2,pairCount);
			Double ar21Weight = computeARWeight(itemCount2,itemCount1,pairCount);
			
			doEmit(key.getBid(),key.getItemId(),key.getAdpos(),otherItem,Constants.cf_alg_name,String.valueOf(key.getGroupId()),cf12Weight);
			doEmit(key.getBid(),otherItem,key.getAdpos(),key.getItemId(),Constants.cf_alg_name,String.valueOf(key.getGroupId()),cf21Weight);
			doEmit(key.getBid(),key.getItemId(),key.getAdpos(),otherItem,Constants.cf_alg_name,String.valueOf(key.getGroupId()),ar12Weight);
			doEmit(key.getBid(),otherItem,key.getAdpos(),key.getItemId(),Constants.ar_alg_name,String.valueOf(key.getGroupId()),ar21Weight);	
		}
		
		private void doEmit(String bid,String itemId,String adpos,String otherItem,  String algName, String groupId, double weight){
			String resultKey = Utils.getAlgKey(bid,itemId, adpos, algName, groupId);		
			Values values = new Values(bid,resultKey,otherItem,weight,algName);
			synchronized(collector){
				collector.emit("computer_result",values);	
			}
			
			if(!groupId.equals("0")){
				String resultKey2 = Utils.getAlgKey(bid,itemId, adpos, algName, "0");		
				Values values2 = new Values(resultKey2,adpos,otherItem,weight,algName);
				synchronized(collector){
					collector.emit("computer_result",values2);	
				}
			}
			
		}
	}
	
	private class GetItemCountCallBack implements MutiClientCallBack{
		private final UpdateKey key;
		private String otherItem;
		private Float itemCount;
		private Integer step;

		public GetItemCountCallBack(UpdateKey key,String otherItem,Float itemCount,Integer step) {
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
			} catch (Exception e){
				logger.error(e.getMessage(), e);
			}
		}

		@Override
		public void handle(Future<?> future, Object context) {			
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			Float count = 0F;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult()!=null){
					count = Float.parseFloat(new String(res.getResult()));
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
			
			if(step == 1){
				new GetItemCountCallBack(key,otherItem,count,2).excute();
				//logger.info("step2,item1="+key.getItemId()+", count="+count);
			}else if(step == 2){
				new GetPairsCountCallBack(key,otherItem,itemCount,count).excute();
				//logger.info("step2,itesm2"+otherItem+", count="+count);
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

			} catch (Exception e) {
				logger.error(e.getMessage(), e);
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
						new GetItemCountCallBack(key,otherItem, 0F,1).excute();
						//logger.info("step1,emit to step2 "+key.getItemId()+" with item ="+otherItem);
					}					
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
			
		}
	}


}