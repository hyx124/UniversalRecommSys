package com.tencent.urs.bolts;

import java.lang.ref.SoftReference;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.tencent.streaming.commons.bolts.config.AbstractConfigUpdateBolt;
import com.tencent.streaming.commons.spouts.tdbank.Output;
import com.tencent.tde.client.Result;
import com.tencent.tde.client.Result.ResultCode;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.ChargeType;
import com.tencent.urs.protobuf.Recommend.ItemDetailInfo;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class AddItemInfoBolt extends AbstractConfigUpdateBolt {
	private static final long serialVersionUID = 1L;
	private List<ClientAttr> mtClientList;	
	private DataCache<ItemDetailInfo> itemCache;
	private OutputCollector collector;
	
	private int cacheExpireTime;
	private int nsTableId;
	private boolean debug;
	
	private static Logger logger = LoggerFactory
			.getLogger(AddItemInfoBolt.class);

	public AddItemInfoBolt(String config, ImmutableList<Output> outputField){
		super(config, outputField, Constants.config_stream);
	}
	
	@Override 
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		super.prepare(conf, context, collector);
		updateConfig(super.config);
		
		this.collector = collector;
		this.itemCache = new DataCache<ItemDetailInfo>(conf);
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		ClientAttr clientEntry = mtClientList.get(0);		
		TairOption opt = new TairOption(clientEntry.getTimeout(),(short)0, 24*3600);
	
	} 
	
	@Override
	public void updateConfig(XMLConfiguration config) {
		nsTableId = config.getInt("item_detail_table",311);
		cacheExpireTime = config.getInt("cache_expiretime",24*3600);
		debug = config.getBoolean("debug",false);
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {
		String bid = tuple.getStringByField("bid");
		String key = tuple.getStringByField("key");	
		String itemId = tuple.getStringByField("item_id");	
		double weight = tuple.getDoubleByField("weight");	
		String algName = tuple.getStringByField("alg_name");	
		
		new GetItemInfoCallBack(bid,key,itemId,weight,algName).excute();
		
	}
	
	public class GetItemInfoCallBack implements MutiClientCallBack{
		private String key;
		private String itemId;
		private String bid;
		private double weight;
		private String algName;
		private String cacheKey;
		
		public GetItemInfoCallBack(String bid,String key,String itemId, double weight, String algName){
			this.bid = bid;
			this.key = key;
			this.itemId = itemId;
			this.weight = weight;
			this.algName = algName;
			
			this.cacheKey = bid+"#"+itemId;
		}
		
		@Override
		public void handle(Future<?> future, Object context) {
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			ItemDetailInfo itemInfo = null;
			try {
				Result<byte[]> res = afuture.get();
				if(res.getCode().equals(ResultCode.OK) && res.getResult() != null){
					itemInfo = Recommend.ItemDetailInfo.parseFrom(res.getResult());
					itemCache.set(cacheKey, new SoftReference<ItemDetailInfo>(itemInfo),cacheExpireTime);
				}
			} catch (Exception e) {
			}
			
			
			emitData(itemInfo);
		}

		private void emitData(ItemDetailInfo itemInfo){
			Values value = new Values(bid,key,itemId,weight,algName);
			if(itemInfo != null){
				value.add(itemInfo.getBigType());
				value.add(itemInfo.getMiddleType());
				value.add(itemInfo.getSmallType());
				value.add(itemInfo.getFreeFlag());
				Float price = itemInfo.getPrice();
				
				value.add(price.longValue());		
			}else{
				value.add(0L);
				value.add(0L);
				value.add(0L);
				value.add(ChargeType.NormalFee);
				value.add(0L);		
			}
			//bid,key,item_id,weight,alg_name,big_type,mid_type,small_type,free,price
			collector.emit("computer_result",value);			
		}
		
		public void excute() {
			ItemDetailInfo itemInfo = null;
			SoftReference<ItemDetailInfo> sr = itemCache.get(cacheKey);
			if(sr != null){
				itemInfo = sr.get();
			}
			
			if(itemInfo != null){
				emitData(itemInfo);
			}else{
				try{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsTableId,cacheKey.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}catch(Exception e){
					logger.error(e.toString());
				}
			}

		}
	}
	
	
	
}