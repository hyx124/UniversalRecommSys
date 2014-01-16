package com.tencent.urs.bolts;

import java.util.HashMap;
import java.util.HashSet;
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
import com.tencent.urs.combine.ActionCombinerValue;
import com.tencent.urs.combine.UpdateKey;
import com.tencent.urs.conf.AlgModuleConf;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.Utils;

public class ARBolt extends AbstractConfigUpdateBolt{
	/**
	 * 
	 */
	private static final long serialVersionUID = -7105767417026977180L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private ConcurrentHashMap<UpdateKey, ActionCombinerValue> combinerMap;
	private DataCache<UserActiveDetail> cacheMap;
	//private DataCache<UserActiveDetail> cacheMap;
	private AlgModuleConf algInfo;
	private int combinerExpireTime;
	private OutputCollector collector;
	
	private static Logger logger = LoggerFactory.getLogger(ARBolt.class);

	@SuppressWarnings("rawtypes")
	public ARBolt(String config, ImmutableList<Output> outputField,
			String sid){
		super(config, outputField, sid);
		this.updateConfig(super.config);
		
	}
	
	@Override 
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		super.prepare(conf, context, collector);
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.combinerMap = new ConcurrentHashMap<UpdateKey,ActionCombinerValue>(1024);

		this.combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5);
		setCombinerTime(combinerExpireTime);
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
							ActionCombinerValue value = combinerMap.remove(key);
							try{
								new GetItemPairsCallBack(key,"").excute();
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
	
	private void combinerKeys(UpdateKey key,ActionCombinerValue value) {
		combinerMap.put(key, value);
	}	
	
	private class GetPairsCountCallBack implements MutiClientCallBack{
		private final UpdateKey key;
		private final Integer itemCount1;
		private final Integer itemCount2;
		private final String otherItem;
		
		public GetPairsCountCallBack(UpdateKey key,String otherItem,Integer itemCount1,Integer itemCount2) {
			this.key = key ; 
			this.otherItem = otherItem;
			this.itemCount1 = itemCount1;
			this.itemCount2 = itemCount2;
		}

		public void excute() {
			try {
				
				String checkKey = key.getUin() + "#DETAIl";				
				ClientAttr clientEntry = mtClientList.get(0);		
				TairOption opt = new TairOption(clientEntry.getTimeout());
				Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)algInfo.getOutputTableId(),checkKey.getBytes(),opt);
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
					String arKey = "item1#adpos#ar";
					String cfKey = "item1#adpos#cf";
					collector.emit("storageDream",new Values(algInfo.getAlgName(),arKey,otherItem,arWeight));
					collector.emit("storageDream",new Values(algInfo.getAlgName(),cfKey,otherItem,cfWeight));
				}
			} catch (Exception e) {
				
			}
			
		}
	}
	
	private class GetItemPairsCallBack implements MutiClientCallBack{
		private final UpdateKey key;
		private final String itemCount;
		public GetItemPairsCallBack(UpdateKey key,String itemCount) {
			this.key = key ; 
			this.itemCount = itemCount;
		}

		public void excute() {
			try {
				String checkKey = key.getUin() + "#DETAIl";
				
				ClientAttr clientEntry = mtClientList.get(0);		
				TairOption opt = new TairOption(clientEntry.getTimeout());
				Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)algInfo.getOutputTableId(),checkKey.getBytes(),opt);
				clientEntry.getClient().notifyFuture(future, this,clientEntry);	
							
				
			} catch (TairQueueOverflow e) {
				//log.error(e.toString());
			} catch (TairRpcError e) {
				//log.error(e.toString());
			} catch (TairFlowLimit e) {
				//log.error(e.toString());
			}
		}
		
		private HashSet<String> getPairItems(UserActiveDetail oldValueHeap, String itemId){
			HashSet<String>  itemSet = new HashSet<String>();		
			
			for(Recommend.UserActiveDetail.TimeSegment tsegs:oldValueHeap.getTsegsList()){
				HashMap<String,Integer>  doWeightMap = null;
				if(!inValidTimeSeg(tsegs.getTimeId())){
					continue;
				}
				
				for(Recommend.UserActiveDetail.TimeSegment.ItemInfo item: tsegs.getItemsList()){
					if(!item.getItem().equals(key.getItemId())){
						itemSet.add(item.getItem());
					}
				}
			}
			return itemSet;
		}

		private boolean inValidTimeSeg(long timeId) {
			// TODO Auto-generated method stub
			return false;
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


	private class GetItemCountCallBack implements MutiClientCallBack{
		private final UpdateKey key;
		private String otherItem;
		private Integer itemCount;
		private Integer count2;
		private Integer step;

		public GetItemCountCallBack(UpdateKey key,String otherItem,Integer itemCount,Integer step) {
			this.key = key ; 
			this.otherItem = otherItem;
			this.itemCount = itemCount;
			this.step = step;
		}

		public void excute() {
			try {
				String checkKey = key.getGroupId() + "#" + key.getItemId() +"#" +key.getAdpos();
				ClientAttr clientEntry = mtClientList.get(0);		
				TairOption opt = new TairOption(clientEntry.getTimeout());
				String tableId = algInfo.getInputTableIdByName("GroupItemCount");
				if(tableId != null){
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync(Short.valueOf(tableId),checkKey.getBytes(),opt);
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


	@Override
	public void updateConfig(XMLConfiguration config) {
		try {
			this.algInfo.load(config);
		} catch (ConfigurationException e) {
			logger.error(e.toString());
		}
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {
		String bid = tuple.getStringByField("bid");
		Long uin = tuple.getLongByField("newqq");
		Integer groupId = tuple.getIntegerByField("groupId");
		String itemId = tuple.getStringByField("itemId");
		String adpos = tuple.getStringByField("adpos");
		
		ActionCombinerValue value = new ActionCombinerValue();
		
		UpdateKey key = new UpdateKey(bid,uin,groupId,adpos,itemId);
		combinerKeys(key,value);	
	}

}