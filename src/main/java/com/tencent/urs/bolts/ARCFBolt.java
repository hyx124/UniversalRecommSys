package com.tencent.urs.bolts;

import java.lang.ref.SoftReference;
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
import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.bolts.PretreatmentBolt.GetGroupIdUpdateCallBack;
import com.tencent.urs.combine.GroupActionCombinerValue;
import com.tencent.urs.combine.UpdateKey;
import com.tencent.urs.protobuf.Recommend.GroupCountInfo;
import com.tencent.urs.protobuf.Recommend.GroupPairInfo;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType;
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
	private DataCache<Float> itemCountCache;
	private DataCache<Float> itemPairCache;
	private OutputCollector collector;
	private int nsGroupPairTableId;
	private int nsGroupCountTableId;
	private int nsDetailTableId;
	private int dataExpireTime;
	private int linkedTime;
	private boolean debug;
	
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
		this.itemCountCache = new DataCache<Float>(conf);
		this.itemPairCache = new DataCache<Float>(conf);
		int combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5)+3;
		setCombinerTime(combinerExpireTime);
	} 
	
	@Override
	public void updateConfig(XMLConfiguration config) {
		nsGroupCountTableId = config.getInt("group_count_table",514);
		nsGroupPairTableId = config.getInt("group_pair_table",516);
		nsDetailTableId = config.getInt("dependent_table",512);
		dataExpireTime = config.getInt("data_expiretime",7*24*3600);
		linkedTime = config.getInt("linked_time",1*24*3600);		
		debug = config.getBoolean("debug",false);
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {
		try{
			String bid = tuple.getStringByField("bid");
			String qq = tuple.getStringByField("qq");
			String groupId = tuple.getStringByField("group_id");
			String itemId = tuple.getStringByField("item_id");
			String adpos = Constants.DEFAULT_ADPOS;
			
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
				Float thisItemPairCount = null;
				SoftReference<Float> sr = itemPairCache.get(getKey);
				if(sr != null){
					thisItemPairCount = sr.get();
				}
					
				if(thisItemPairCount != null ){

					if(debug){
						logger.info("step3,key"+getKey+"item1="+key.getItemId()+
								",item2="+otherItem+"itemcount1="+itemCount1+
								",itemcount2="+itemCount2+",paircount="+thisItemPairCount);
					}
					Double cf12Weight = computeCFWeight(itemCount1,itemCount2,thisItemPairCount);
					Double cf21Weight = computeCFWeight(itemCount2,itemCount1,thisItemPairCount);
					
					doEmit(key.getBid(),key.getItemId(),key.getAdpos(),otherItem,String.valueOf(key.getGroupId()),cf12Weight);
					doEmit(key.getBid(),otherItem,key.getAdpos(),key.getItemId(),String.valueOf(key.getGroupId()),cf21Weight);
					
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsGroupPairTableId,getKey.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this, clientEntry);	
				}	
		
			} catch (Exception e){
				logger.error(e.getMessage(), e);
			}
		}
		
		public Double computeCFWeight(Float itemCount1, Float itemCount2,
				Float pairCount) {	
			if(itemCount1 == 0 || itemCount2 == 0 || pairCount == 0){
				return 0D;
			}
			
			Double simCF = (double) (pairCount/(Math.sqrt(itemCount1) * Math.sqrt(itemCount2)));
			return simCF;
		}
		
		@Override
		public void handle(Future<?> future, Object context) {			
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			GroupPairInfo weightInfo = null;
			try {
				Result<byte[]> res = afuture.get();	
				if(res.isSuccess() && res.getResult()!=null){
					weightInfo = GroupPairInfo.parseFrom(res.getResult());
				}
			} catch (Exception e){
				logger.error(e.getMessage(), e);
			}
			Float pairCount = getWeight(weightInfo);
			itemPairCache.set(getKey, new SoftReference<Float>(pairCount),5);
			if(debug){
				logger.info("step3,key"+getKey+"item1="+key.getItemId()+
						",item2="+otherItem+"itemcount1="+itemCount1+
						",itemcount2="+itemCount2+",paircount="+pairCount);
			}
			Double cf12Weight = computeCFWeight(itemCount1,itemCount2,pairCount);
			Double cf21Weight = computeCFWeight(itemCount2,itemCount1,pairCount);
			
			doEmit(key.getBid(),key.getItemId(),key.getAdpos(),otherItem,String.valueOf(key.getGroupId()),cf12Weight);
			doEmit(key.getBid(),otherItem,key.getAdpos(),key.getItemId(),String.valueOf(key.getGroupId()),cf21Weight);
		}
		
		private Float getWeight(GroupPairInfo weightInfo){
			Float sumCount = 0F;
			Long now = System.currentTimeMillis()/1000;
			if(weightInfo != null){
				for(GroupPairInfo.TimeSegment ts: weightInfo.getTsegsList()){
					if(ts.getTimeId() >= Utils.getDateByTime(now - dataExpireTime)){
						sumCount += ts.getCount();
					}
				}
			}

			return sumCount;
		}
		
		private void doEmit(String bid,String itemId,String adpos,String otherItem, String groupId, double weight){
			if(weight <= 0){
				return;
			}
			
			String resultKey = Utils.getAlgKey(bid,itemId, adpos, Constants.cf_alg_name, groupId);	
			Values values = new Values(bid,resultKey,otherItem,weight,Constants.cf_alg_name);
			if(debug){
				logger.info("step3,output result,key"+resultKey+",otherItem="+otherItem+",weight="+weight);
			}
			
			synchronized(collector){
				collector.emit(Constants.alg_result_stream,values);	
			}
			
			if(!groupId.equals("0")){
				String resultKey2 = Utils.getAlgKey(bid,itemId, adpos, Constants.cf_nogroup_alg_name, "0");		
				Values values2 = new Values(bid,resultKey2,otherItem,weight,Constants.cf_nogroup_alg_name);
				synchronized(collector){
					collector.emit(Constants.alg_result_stream,values2);	
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
		
				Float thisItemCount = null;
				SoftReference<Float> sr = itemCountCache.get(getKey);
				if(sr != null){
					thisItemCount = sr.get();
				}
					
				if(thisItemCount != null ){
					if(step == 1){
						new GetItemCountCallBack(key,otherItem,thisItemCount,2).excute();
					}else if(step == 2){
						new GetPairsCountCallBack(key,otherItem,itemCount,thisItemCount).excute();	
					}		
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsGroupCountTableId,getKey.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);		
				}
			} catch (Exception e){
				logger.error(e.getMessage(), e);
			}
		}

		@Override
		public void handle(Future<?> future, Object context) {			
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			GroupCountInfo weightInfo = null;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult()!=null){
					weightInfo = GroupCountInfo.parseFrom(res.getResult());
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
			
			Float count = getWeight(weightInfo);
			
			if(step == 1){
				new GetItemCountCallBack(key,otherItem,count,2).excute();
				itemCountCache.set(key.getGroupCountKey(), new SoftReference<Float>(count),5);
			}else if(step == 2){
				new GetPairsCountCallBack(key,otherItem,itemCount,count).excute();	
				itemCountCache.set(key.getOtherGroupCountKey(otherItem), new SoftReference<Float>(count),5);
			}
		}
		
		private Float getWeight(GroupCountInfo weightInfo){
			Float sumCount = 0F;
			Long now = System.currentTimeMillis()/1000;
			if(weightInfo != null){
				
				for(GroupCountInfo.TimeSegment ts: weightInfo.getTsegsList()){
					if(ts.getTimeId() >= Utils.getDateByTime(now - dataExpireTime)){
						sumCount += ts.getCount();
					}
				}
			}
			return sumCount;
		}
	}
	
	private class GetPairsListCallBack implements MutiClientCallBack{
		private final UpdateKey key;
		private GroupActionCombinerValue value;
		public GetPairsListCallBack(UpdateKey key,GroupActionCombinerValue value) {
			this.key = key ; 
			this.value = value;
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
		
		
		private HashSet<String> getPairItems(UserActiveDetail oldValueHeap, String itemId){
			HashSet<String>  itemSet = new HashSet<String>();		
			
			for(UserActiveDetail.TimeSegment tsegs:oldValueHeap.getTsegsList()){
				if(tsegs.getTimeId() >= Utils.getDateByTime(value.getTime() - linkedTime)){
					for(UserActiveDetail.TimeSegment.ItemInfo item: tsegs.getItemsList()){
						ActType  maxType = getMaxActFromType(item.getActsList());
						if(Utils.getActionWeight(maxType.getActType()) == 0){
							continue;
						}
						
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
					UserActiveDetail oldValueHeap = UserActiveDetail.parseFrom(result.getResult());
					HashSet<String> itemSet = getPairItems(oldValueHeap,key.getItemId());

					for(String otherItem:itemSet){
						new GetItemCountCallBack(key,otherItem, 0F,1).excute();
						if(debug){
							logger.info("step1,emit to step2 "+key.getItemId()+" with item ="+otherItem+",uin="+key.getUin());
						}
					}					
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
			
		}
	}


}