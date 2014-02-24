package com.tencent.urs.bolts;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.google.common.collect.ImmutableList;
import com.tencent.monitor.MonitorTools;
import com.tencent.streaming.commons.bolts.config.AbstractConfigUpdateBolt;
import com.tencent.streaming.commons.spouts.tdbank.Output;
import com.tencent.tde.client.Result;
import com.tencent.tde.client.TairClient.TairOption;

import com.tencent.urs.asyncupdate.UpdateCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBackContext;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;

public class BaseInfoBolt extends AbstractConfigUpdateBolt{
	private static final long serialVersionUID = -1302335947421120663L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private UpdateCallBack putCallBack;
	private int nsItemDetailTableId;
	private int nsUserDetailTableId;
	private int nsActionWeightTableId;
	private int nsCateLevelTableId;
	private int dataExpireTime;
	private boolean debug;
	
	private static Logger logger = LoggerFactory
			.getLogger(BaseInfoBolt.class);
	
	@SuppressWarnings("rawtypes")
	public BaseInfoBolt(String config, ImmutableList<Output> outputField){
		super(config, outputField, Constants.config_stream);
				
	}
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		super.prepare(conf, context, collector);
		updateConfig(super.config);
				
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_interfaceID, this.getClass().getName());
	}
	
	private void save(short tableId,String key,byte[] values) {
		if(values != null){
			logger.info("key="+key+",tableId="+tableId);
			for(ClientAttr clientEntry:mtClientList){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);				
				try {
					Future<Result<Void>> future = clientEntry.getClient().putAsync(tableId, 
										key.getBytes(), values, putopt);
					
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,key,values,putopt));
				} catch (Exception e){
					logger.error(e.getMessage(), e);
				}	
			}
		}
	}

	private byte[] genItemInfoPbValue(String itemId,Tuple input) {
		//String itemId = input.getStringByField("item_id");
		String impDate = input.getStringByField("imp_date");
		String bigType = input.getStringByField("cate_id1");
		String midType = input.getStringByField("cate_id2");
		String smallType = input.getStringByField("cate_id3");
		String bigTypeName = input.getStringByField("cate_name1");
		String midTypeName = input.getStringByField("cate_name2");
		String smallTypeName = input.getStringByField("cate_name3");
		String freeFlag_str = input.getStringByField("free");
		Recommend.ChargeType freeFlag = Recommend.ChargeType.NormalFee;
		if(freeFlag_str.equals("1")){
			freeFlag = Recommend.ChargeType.Free;
		}else if(freeFlag_str.equals("2")){
			freeFlag = Recommend.ChargeType.VipFree;
		}
		
		String publicFlag_str = input.getStringByField("publish");
		Recommend.ItemDetailInfo.PublicType publicFlag = Recommend.ItemDetailInfo.PublicType.NotPublic;
		if(publicFlag_str.equals("1")){
			publicFlag = Recommend.ItemDetailInfo.PublicType.OnSell;
		}else if(publicFlag_str.equals("2")){
			publicFlag = Recommend.ItemDetailInfo.PublicType.SellOut;
		}
		
		
		String price = input.getStringByField("price");
		String text = input.getStringByField("text");
		
		String itemTime = input.getStringByField("item_time");
		String platForm = input.getStringByField("plat_form");
		String score = input.getStringByField("score");
		String shopId = input.getStringByField("shop_id");
		
		Recommend.ItemDetailInfo.Builder builder =
				Recommend.ItemDetailInfo.newBuilder();
		builder.setItem(itemId).setFreeFlag(freeFlag).setPublicFlag(publicFlag).setImpDate(Long.valueOf(impDate))
				.setPrice(Float.parseFloat(price)).setText(text).setShopId(shopId)
				.setItemTime(Long.valueOf(itemTime)).setPlatform(Long.valueOf(platForm)).setScore(Long.valueOf(score))
				.setBigType(Long.valueOf(bigType)).setBigTypeName(bigTypeName)
				.setMiddleType(Long.valueOf(midType)).setMiddleTypeName(midTypeName)
				.setSmallType(Long.valueOf(smallType)).setSmallTypeName(smallTypeName);
		
		return builder.build().toByteArray();
	}

	private byte[] genCateLevelPbValue(String cate_id, Tuple input) {
		String impDate = input.getStringByField("imp_date");
		String cateName = input.getStringByField("cate_name");
		String level = input.getStringByField("level");
		String fatherId = input.getStringByField("father_id");
		
		Recommend.CateLevelInfo.Builder builder = Recommend.CateLevelInfo.newBuilder();
		builder.setImpDate(Long.valueOf(impDate)).setLevel(level).setName(cateName).setFatherId(Integer.valueOf(fatherId));
		
		return builder.build().toByteArray();
	}

	private byte[] genActionWeightPbValue(String actionType, Tuple input) {
		String impDate = input.getStringByField("imp_date");
		String weight = input.getStringByField("weight");
		
		Recommend.ActionWeightInfo.Builder builder = Recommend.ActionWeightInfo.newBuilder();
		builder.setImpDate(Long.valueOf(impDate)).setWeight(Float.valueOf(weight));	
		
		return builder.build().toByteArray();
	}

	private byte[] genUserInfoPbValue(String qq, Tuple input) {
		String impDate = input.getStringByField("imp_date");
		String imei = input.getStringByField("imei");
		String uid = input.getStringByField("uid");
		String level = input.getStringByField("level");
		String regDate = input.getStringByField("reg_date");
		String regTime = input.getStringByField("reg_time");
		
		Recommend.UserDetailInfo.Builder builder = Recommend.UserDetailInfo.newBuilder();
		builder.setImpDate(Long.valueOf(impDate)).setImei(imei)
				.setQQNum(qq).setLevel(Integer.valueOf(level)).setUid(uid)
				.setRegDate(Long.valueOf(regDate)).setRegTime(Long.valueOf(regTime));
		return builder.build().toByteArray();
	}

	@Override
	public void updateConfig(XMLConfiguration config) {
		nsItemDetailTableId = config.getInt("item_detail_table",311);
		nsUserDetailTableId = config.getInt("user_detail_table",312);
		nsActionWeightTableId = config.getInt("action_weight_table",313);
		nsCateLevelTableId = config.getInt("category_level_table",314);
		
		dataExpireTime = config.getInt("data_expiretime",10*24*3600);
		debug = config.getBoolean("debug",false);
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {
		String bid = tuple.getStringByField("bid");
		String topic = tuple.getStringByField("topic");
		
		Integer tableId = null;
		String key = null;
		byte[] value = null;
		
		if(topic.equals(Constants.item_info_stream)){
			String itemId = tuple.getStringByField("item_id");
			key = bid+"#"+itemId;
			tableId = nsItemDetailTableId;
			
			value = genItemInfoPbValue(itemId,tuple);		
		}else if(topic.equals(Constants.user_info_stream)){
			String qq = tuple.getStringByField("qq");
			key = bid+"#"+qq;
			tableId = nsUserDetailTableId;
			
			value = genUserInfoPbValue(qq,tuple);
		}else if(topic.equals(Constants.action_weight_stream)){
			String actionType = tuple.getStringByField("type_id");
			key = bid+"#"+actionType;
			tableId = nsActionWeightTableId;
			
			value = genActionWeightPbValue(actionType,tuple);
		}else if(topic.equals(Constants.category_level_stream)){
			String cate_id = tuple.getStringByField("cate_id");
			key = bid+"#"+cate_id;
			tableId = nsCateLevelTableId;
			
			value = genCateLevelPbValue(cate_id,tuple);
		}else{
			return;
		}
		
		save(tableId.shortValue(),key,value);
	}

}