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
import com.tencent.urs.utils.Utils;

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
	} 
	
	@Override
	public void updateConfig(XMLConfiguration config) {
		nsTableId = config.getInt("item_detail_table",521);
		cacheExpireTime = config.getInt("cache_expiretime",24*3600);
		debug = config.getBoolean("debug",false);
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {
		try{
			String bid = tuple.getStringByField("bid");
			String itemId = tuple.getStringByField("item_id");	
			
			if(Utils.isBidValid(bid) && Utils.isItemIdValid(itemId)){
				new GetItemInfoCallBack(sid, bid, itemId,tuple).excute();
			}	
		}catch(Exception e){
			logger.error(e.getMessage(), e);
		}
	
	}
	
	public class GetItemInfoCallBack implements MutiClientCallBack{
		private String cacheKey;
		private String sid;
		private Tuple tuple;
		private String bid;
		
		public GetItemInfoCallBack(String sid ,String bid, String itemId, Tuple tuple){	
			this.sid = sid;
			this.bid = bid;
			this.tuple = tuple;
			this.cacheKey = bid+"#"+itemId;
		}
		
		@Override
		public void handle(Future<?> future, Object context) {
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			ItemDetailInfo itemInfo = null;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() != null){
					itemInfo = Recommend.ItemDetailInfo.parseFrom(res.getResult());
					itemCache.set(cacheKey, new SoftReference<ItemDetailInfo>(itemInfo),cacheExpireTime);
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
			emitData(itemInfo);
		}

		private void emitData(ItemDetailInfo itemInfo){	
			Values value = null;
			String streamId = null;
			if(sid.equals(Constants.actions_stream)){
				String bid = tuple.getStringByField("bid");
				String qq = tuple.getStringByField("qq");
				String itemId = tuple.getStringByField("item_id");
				
				String actionType = tuple.getStringByField("action_type");
				String actionTime = tuple.getStringByField("action_time");
				String lbsInfo = tuple.getStringByField("lbs_info");
				String platform = tuple.getStringByField("platform");
				
				streamId = Constants.actions_stream;
				value = new Values(bid,itemId,qq,actionType,actionTime,platform,lbsInfo);	
			}else if(sid.equals(Constants.alg_result_stream)){
				String algName = this.tuple.getStringByField("alg_name");	
				String key = tuple.getStringByField("key");
				String itemId = tuple.getStringByField("item_id");
				double weight = tuple.getDoubleByField("weight");
				
				if(algName.equals(Constants.cate_alg_name)){
					if(key.indexOf("Big-Type") > 0 && itemInfo.getBigType() > 0){
						key = key.replace("Big-Type", String.valueOf(itemInfo.getBigType()));
					}else if(key.indexOf("Mid-Type") > 0 && itemInfo.getMiddleType() > 0){
						key = key.replace("Mid-Type", String.valueOf(itemInfo.getMiddleType()));
					}else if(key.indexOf("Small-Type") > 0 && itemInfo.getMiddleType() > 0){
						key = key.replace("Small-Type", String.valueOf(itemInfo.getMiddleType()));
					}else{
						if(debug){
							logger.info("key="+key+",itemId="+itemId+",itemInfo.bigType="+itemInfo.getBigType());
						}
						return;
					}
				}
	
				streamId = Constants.alg_result_stream;
				value = new Values(bid,key,itemId,weight,algName);
			}else{
				return;
			}
			
			if(itemInfo != null){
				value.add(itemInfo.getBigType());
				value.add(itemInfo.getMiddleType());
				value.add(itemInfo.getSmallType());
				value.add(itemInfo.getFreeFlag());
			
				Float price = itemInfo.getPrice();
				
				value.add(price.longValue());	
				value.add(itemInfo.getShopId());
			}else{
				value.add(0L);
				value.add(0L);
				value.add(0L);
				value.add(ChargeType.NormalFee);
				value.add(0L);	
				value.add("");
			}
			
			if(value!=null && streamId != null){
				synchronized(collector){
					collector.emit(streamId,value);	
				}
			}		
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
					logger.error(e.getMessage(), e);
				}
			}

		}
	}
	
}