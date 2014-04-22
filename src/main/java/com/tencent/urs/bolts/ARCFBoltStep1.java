package com.tencent.urs.bolts;

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
import com.tencent.urs.combine.GroupActionCombinerValue;
import com.tencent.urs.combine.UpdateKey;
import com.tencent.urs.protobuf.Recommend.GroupCountInfo;
import com.tencent.urs.protobuf.Recommend.GroupPairInfo;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.Utils;

public class ARCFBoltStep1 extends AbstractConfigUpdateBolt{
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
	private int linkedTime;
	private boolean debug;
	
	private static Logger logger = LoggerFactory.getLogger(ARCFBoltStep1.class);

	@SuppressWarnings("rawtypes")
	public ARCFBoltStep1(String config, ImmutableList<Output> outputField){
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
		
		private HashSet<String> getPairItems(UserActiveDetail oldValueHeap, String itemId){
			HashSet<String>  itemSet = new HashSet<String>();		
			
			for(UserActiveDetail.TimeSegment tsegs:oldValueHeap.getTsegsList()){
				if(tsegs.getTimeId() >= Utils.getDateByTime(value.getTime() - linkedTime)){
					for(UserActiveDetail.TimeSegment.ItemInfo item: tsegs.getItemsList()){
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
						String hashKey = key.getGroupId()+"#" +Utils.getItemPairKey(key.getItemId(),otherItem);
						
						synchronized(collector){
							collector.emit(Constants.alg_result_stream,new Values(hashKey,key.getGroupId(),key.getItemId(),otherItem));	
						}
					}					
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
			
		}
	}


}