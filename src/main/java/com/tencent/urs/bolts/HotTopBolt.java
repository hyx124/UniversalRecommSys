package com.tencent.urs.bolts;

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
import com.tencent.urs.protobuf.Recommend.ActiveType;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.Utils;

public class HotTopBolt extends AbstractConfigUpdateBolt{

	private static final long serialVersionUID = 4730598061697463554L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private ConcurrentHashMap<UpdateKey, GroupActionCombinerValue> combinerMap;
	private int combinerExpireTime;
	private OutputCollector collector;
	private int nsGroupCountTableId;
	private String categoryType;

	
	private static Logger logger = LoggerFactory
			.getLogger(HotTopBolt.class);
	
	
	public HotTopBolt(String config, ImmutableList<Output> outputField) {
		super(config, outputField, Constants.config_stream);
	}

	@Override
	public void updateConfig(XMLConfiguration config) {
		nsGroupCountTableId = config.getInt("item_count_table",304);
		categoryType = config.getString("category_type","Small-Type");
		//dataExpireTime = config.getInt("data_expiretime",1*24*3600);
		//cacheExpireTime = config.getInt("cache_expiretime",3600);
	}
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		super.prepare(conf, context, collector);
		this.updateConfig(super.config);

		this.collector = collector;
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.combinerMap = new ConcurrentHashMap<UpdateKey,GroupActionCombinerValue>(1024);
			
		this.combinerExpireTime = Utils.getInt(conf, "combiner.expireTime",5);
		setCombinerTime(combinerExpireTime);
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {			
		String bid = tuple.getStringByField("bid");
		String qq = tuple.getStringByField("qq");
		String groupId = tuple.getStringByField("group_id");
		String adpos = tuple.getStringByField("adpos");
		String itemId = tuple.getStringByField("item_id");
		
		String actionType = tuple.getStringByField("action_type");
		String actionTime = tuple.getStringByField("action_time");
	
		ActiveType actType = Utils.getActionTypeByString(actionType);
		
		if(!Utils.isBidValid(bid) || !Utils.isQNumValid(qq) || !Utils.isGroupIdVaild(groupId) || !Utils.isItemIdValid(itemId)){
			return;
		}
		
		GroupActionCombinerValue value = new GroupActionCombinerValue(actType,Long.valueOf(actionTime));
		UpdateKey key = new UpdateKey(bid,Long.valueOf(qq),Integer.valueOf(groupId),adpos,itemId);
		combinerKeys(key,value);		
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
							combinerMap.remove(key);
							try{
								new GetGroupCountCallBack(key).excute();
							}catch(Exception e){
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

	private class GetGroupCountCallBack implements MutiClientCallBack{
		private final UpdateKey key;
		private String getKey;

		public GetGroupCountCallBack(UpdateKey key) {
			this.key = key ; 
			this.getKey = key.getGroupCountKey();
		}

		public void excute() {
			try {				
				ClientAttr clientEntry = mtClientList.get(0);		
				TairOption opt = new TairOption(clientEntry.getTimeout());
				Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsGroupCountTableId,getKey.getBytes(),opt);
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
				Result<byte[]> result = afuture.get();	
				if(result.isSuccess() && result.getResult()!=null){
					Double count = Double.parseDouble(new String(result.getResult()));
					//bid,key,item_id,weight,alg_name
					
					String algKey1 = key.getBid()+"#0#"+key.getAdpos()+"#"+Constants.ht_alg_name+"#"+key.getGroupId();
					String algKey2 = key.getBid()+"#0#"+key.getAdpos()+"#"+Constants.cate_alg_name+"#"+key.getGroupId();
					synchronized(collector){
						collector.emit("computer_result",new Values(key.getBid(),algKey1,key.getItemId(),count,Constants.ht_alg_name));
						collector.emit("computer_result",new Values(key.getBid(),algKey2,key.getItemId(),count,Constants.cate_alg_name));
					}
					if(!key.getGroupId().equals("0")){
						String algKey3 =  key.getBid()+"#0#"+key.getAdpos()+"#"+Constants.ht_alg_name+"#0";
						String algKey4 =  key.getBid()+"#0#"+key.getAdpos()+"#"+Constants.cate_alg_name+"#0";
						synchronized(collector){
							collector.emit("computer_result",new Values(key.getBid(),algKey3,key.getItemId(),count,Constants.ht_alg_name));
							collector.emit("computer_result",new Values(key.getBid(),algKey4,key.getItemId(),count,Constants.cate_alg_name));
						}
					}
					
				}else{
					logger.info("parse failed");
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
	}
	
}