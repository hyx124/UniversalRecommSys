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
import com.google.protobuf.InvalidProtocolBufferException;
import com.tencent.monitor.MonitorTools;

import com.tencent.streaming.commons.bolts.config.AbstractConfigUpdateBolt;
import com.tencent.streaming.commons.spouts.tdbank.Output;
import com.tencent.tde.client.Result;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.error.TairFlowLimit;
import com.tencent.tde.client.error.TairQueueOverflow;
import com.tencent.tde.client.error.TairRpcError;
import com.tencent.tde.client.error.TairTimeout;
import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBackContext;
import com.tencent.urs.combine.GroupActionCombinerValue;
import com.tencent.urs.combine.UpdateKey;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.ActiveType;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.Utils;

public class ARCFBolt extends AbstractConfigUpdateBolt{
	/**
	 * 
	 */
	private static final long serialVersionUID = -7105767417026977180L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private ConcurrentHashMap<UpdateKey, GroupActionCombinerValue> combinerMap;
	private DataCache<UserActiveDetail> cacheMap;
	//private DataCache<UserActiveDetail> cacheMap;
	private OutputCollector collector;
	private int nsGroupPairTableId;
	private int nsGroupCountTableId;
	private int nsDetailTableId;
	private int dataExpireTime;
	private int cacheExpireTime;
	
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
		nsGroupPairTableId = config.getInt("pair_count_table",303);
		nsGroupCountTableId = config.getInt("item_count_table",304);
		nsDetailTableId = config.getInt("dependent_table",302);
		dataExpireTime = config.getInt("data_expiretime",1*24*3600);
		cacheExpireTime = config.getInt("cache_expiretime",3600);
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {
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
		
		//GroupActionCombinerValue value = new GroupActionCombinerValue(actType,Long.valueOf(actionTime));
		UpdateKey key = new UpdateKey(bid,Long.valueOf(qq),Integer.valueOf(groupId),adpos,"345");
		//combinerKeys(key,value);	
		String testKey = bid+"#"+key.getItemId()+"#adpos#AR#"+key.getGroupId();
		Double weight = (double) (1000-Long.valueOf(actionTime)%100);
		Values values_cf = new Values(testKey,key.getItemId()+Long.valueOf(actionTime)%100,weight,"CF");
		//this.collector.emit("computer_result",values_cf);
		//test(key,actionTime);
		this.collector.emit("computer_result",values_cf);
	}
	
	private void test(UpdateKey key,String actionTime){
		Recommend.RecommendResult.Builder HeapBuilder = Recommend.RecommendResult.newBuilder();
		String testKey = "345#"+key.getAdpos()+"#AR#"+key.getGroupId();
		for(int i=0;i<=100;i++){
			Recommend.RecommendResult.Result.Builder valueBuilder = Recommend.RecommendResult.Result.newBuilder();
			valueBuilder.setItem(key.getItemId()+String.valueOf(i)).setWeight(1000-i);
			HeapBuilder.addResults(valueBuilder.build());
		}
		
		
		for(ClientAttr clientEntry:mtClientList ){
			logger.info("start in save,client="+clientEntry.getGroupname()+",kye="+testKey+",count="+HeapBuilder.getResultsCount());
			TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
			try {
				Future<Result<Void>> future = 
				clientEntry.getClient().putAsync((short)310, 
						testKey.getBytes(), HeapBuilder.build().toByteArray(), putopt);
			} catch (Exception e){
				logger.error(e.toString());
			}
		}
		
		TairOption getopt = new TairOption(mtClientList.get(0).getTimeout());
		try {
			Result<byte[]> res = mtClientList.get(0).getClient().get((short)310, testKey.getBytes(), getopt);
			if(res.isSuccess() && res.getResult() != null)
			{
				Recommend.RecommendResult oldHeap = Recommend.RecommendResult.parseFrom(res.getResult());
				for( Recommend.RecommendResult.Result each:oldHeap.getResultsList() ){
					logger.info("from tde:itemid="+each.getItem()+",weight="+each.getWeight());
				}
			}			
		} catch (TairRpcError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TairFlowLimit e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TairTimeout e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
				//log.error(e.toString());
			} catch (TairRpcError e) {
				//log.error(e.toString());
			} catch (TairFlowLimit e) {
				//log.error(e.toString());
			}
		}


		public Double computeARWeight(Integer itemCount1, Integer itemCount2,
				Integer pairCount) {	
			//AR
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
			try {
				Result<byte[]> result = afuture.get();	
				if(result.isSuccess() && result.getResult()!=null){
					Integer  pairCount = Integer.valueOf(new String(result.getResult()));
					Double arWeight = computeARWeight(itemCount1,itemCount2,pairCount);
					Double cfWeight = computeCFWeight(itemCount1,itemCount2,pairCount);
					
					
					String key1 = Utils.getAlgKey(key.getBid(), key.getItemId(), key.getAdpos(), "AR", String.valueOf(key.getGroupId()));		
					Values values_ar1 = new Values(key1,key.getAdpos(),otherItem,arWeight,"AR");
					collector.emit("computer_result",values_ar1);
					
					String key2 = Utils.getAlgKey(key.getBid(), key.getItemId(), key.getAdpos(), "CF", String.valueOf(key.getGroupId()));
					Values values_cf2 = new Values(key2,key.getAdpos(),otherItem,cfWeight,"CF");
					collector.emit("computer_result",values_cf2);
					
					
					String key3 = Utils.getAlgKey(key.getBid(), otherItem, key.getAdpos(), "AR", String.valueOf(key.getGroupId()));
					Values values_ar3 = new Values(key3,key.getAdpos(),key.getItemId(),arWeight,"AR");
					collector.emit("computer_result",values_ar3);
					
					
					String key4 = Utils.getAlgKey(key.getBid(), otherItem, key.getAdpos(), "CF", String.valueOf(key.getGroupId()));
					Values values_cf4 = new Values(key4,key.getAdpos(),key.getItemId(),cfWeight,"CF");
					collector.emit("computer_result",values_cf4);
					
				}
			} catch (Exception e) {
				
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
			try {
				if(afuture.get().isSuccess() && afuture.get().getResult()!=null){
					String count = new String(afuture.get().getResult());
					if(step == 1){
						new GetItemCountCallBack(key,otherItem,Integer.valueOf(count),2).excute();
					}else if(step == 2){
						new GetPairsCountCallBack(key,otherItem,itemCount,Integer.valueOf(count)).excute();
					}
					
				}
			} catch (Exception e) {
				
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