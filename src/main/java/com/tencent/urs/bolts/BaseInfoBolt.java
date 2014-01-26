package com.tencent.urs.bolts;

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

import com.google.common.collect.ImmutableList;
import com.tencent.monitor.MonitorTools;
import com.tencent.streaming.commons.bolts.config.AbstractConfigUpdateBolt;
import com.tencent.streaming.commons.spouts.tdbank.Output;
import com.tencent.tde.client.Result;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.error.TairFlowLimit;
import com.tencent.tde.client.error.TairQueueOverflow;
import com.tencent.tde.client.error.TairRpcError;

import com.tencent.urs.asyncupdate.UpdateCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBackContext;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.ActiveType;
import com.tencent.urs.protobuf.Recommend.ChargeType;
import com.tencent.urs.protobuf.Recommend.ItemDetailInfo.PublicType;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.Utils;

public class BaseInfoBolt extends AbstractConfigUpdateBolt{
	private static final long serialVersionUID = -1302335947421120663L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private UpdateCallBack putCallBack;
	private int nsTableID;
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
	
	private void save(Integer tablId,String key,byte[] values) {
		for(ClientAttr clientEntry:mtClientList){
			TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
			Future<Result<Void>> future;
			try {
				future = clientEntry.getClient().putAsync((short)nsTableID, 
									key.toString().getBytes(), values.toString().getBytes(), putopt);
				clientEntry.getClient().notifyFuture(future, putCallBack, 
						new UpdateCallBackContext(clientEntry,key.toString(),values.toString().getBytes(),putopt));
			} catch (Exception e){
				logger.error(e.toString());
			}	
		}
	}

	private byte[] genItemInfoPbValue(String itemId,Tuple input) {
		//String itemId = input.getStringByField("item_id");
		Long impDate = input.getLongByField("imp_date");
		String bigType = input.getStringByField("cate_id1");
		String midType = input.getStringByField("cate_id2");
		String smallType = input.getStringByField("cate_id3");
		String bigTypeName = input.getStringByField("cate_name1");
		String midTypeName = input.getStringByField("cate_name2");
		String smallTypeName = input.getStringByField("cate_name3");
		ChargeType freeFlag = (Recommend.ChargeType) input.getValueByField("free");
		PublicType publicFlag = (Recommend.ItemDetailInfo.PublicType) input.getValueByField("publish");
		Float price = input.getFloatByField("price");
		String text = input.getStringByField("text");
		
		Long itemTime = input.getLongByField("item_time");
		Long platForm = input.getLongByField("plat_form");
		Long score = input.getLongByField("score");
		
		Recommend.ItemDetailInfo.Builder builder =
				Recommend.ItemDetailInfo.newBuilder();
		builder.setItem(itemId).setFreeFlag(freeFlag).setPublicFlag(publicFlag).setImpDate(impDate)
				.setPrice(price).setText(text).setItemTime(itemTime).setPlatform(platForm).setScore(score)
				.setBigType(Long.valueOf(bigType)).setBigTypeName(bigTypeName)
				.setMiddleType(Long.valueOf(midType)).setMiddleTypeName(midTypeName)
				.setSmallType(Long.valueOf(smallType)).setSmallTypeName(smallTypeName);
		
		return builder.build().toByteArray();
	}


	private byte[] genCateLevelPbValue(String cate_id, Tuple input) {
		Long impDate = input.getLongByField("imp_date");
		String cateName = input.getStringByField("cate_name");
		String level = input.getStringByField("level");
		String bigTypeName = input.getStringByField("father_id");
		String midTypeName = input.getStringByField("cate_name2");
		String smallTypeName = input.getStringByField("cate_name3");
		
		return null;
	}

	private byte[] genActionWeightPbValue(ActiveType actionType, Tuple input) {
		
		return null;
	}

	private byte[] genUserInfoPbValue(String qq, Tuple input) {
		Long impDate = input.getLongByField("imp_date");
		String imei = input.getStringByField("imei");
		String uid = input.getStringByField("uid");
		Integer level = input.getIntegerByField("level");
		Long regDate = input.getLongByField("reg_date");
		Long regTime = input.getLongByField("reg_time");
		
		Recommend.UserDetailInfo.Builder builder = Recommend.UserDetailInfo.newBuilder();
		builder.setImpDate(impDate).setImei(imei)
				.setQQNum(qq).setLevel(level).setUid(uid)
				.setRegDate(regDate).setRegTime(regTime);
		return builder.build().toByteArray();
	}

	
	@Override
	public void updateConfig(XMLConfiguration config) {
		nsItemDetailTableId = config.getInt("item_detail_table",311);
		nsUserDetailTableId = config.getInt("user_detail_table",312);
		nsActionWeightTableId = config.getInt("action_weight_table",313);
		nsCateLevelTableId = config.getInt("category_level_table",314);
		
		dataExpireTime = config.getInt("data_expiretime",100*24*3600);
		debug = config.getBoolean("debug",false);
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {
		String bid = tuple.getStringByField("bid");
		String topic = tuple.getStringByField("topic");
		
		Integer tableId = null;
		String key = null;
		byte[] value = null;
		
		if(topic.equals("item_detail_info")){
			String itemId = tuple.getStringByField("item_id");
			key = bid+"#"+itemId;
			tableId = nsItemDetailTableId;
			
			value = genItemInfoPbValue(itemId,tuple);		
		}else if(topic.equals("user_detail_info")){
			String qq = tuple.getStringByField("qq");
			key = bid+"#"+qq;
			tableId = nsUserDetailTableId;
			
			value = genUserInfoPbValue(qq,tuple);
		}else if(topic.equals("action_weight_info")){
			Recommend.ActiveType actionType = (Recommend.ActiveType) tuple.getValueByField("action_type");
			key = bid+"#"+actionType;
			tableId = nsActionWeightTableId;
			
			value = genActionWeightPbValue(actionType,tuple);
		}else if(topic.equals("category_level_info")){
			String cate_id = tuple.getStringByField("cate_id");
			key = bid+"#"+cate_id;
			tableId = nsCateLevelTableId;
			
			value = genCateLevelPbValue(cate_id,tuple);
		}else{
			return;
		}
		
		save(tableId,key,value);
		//
	}

}