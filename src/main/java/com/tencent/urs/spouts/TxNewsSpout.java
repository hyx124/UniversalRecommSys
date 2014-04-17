package com.tencent.urs.spouts;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.taobao.metamorphosis.Message;
import com.tencent.streaming.commons.spouts.tdbank.Output;
import com.tencent.streaming.commons.spouts.tdbank.TdbankSpout;
import com.tencent.streaming.commons.spouts.tdbank.TDMsg1;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import com.tencent.monitor.MonitorEntry;
import com.tencent.monitor.MonitorTools;
import com.tencent.urs.conf.DataFilterConf;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;

import backtype.storm.tuple.Values;

import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.Utils;

public class TxNewsSpout extends TdbankSpout {
	
	private static final long serialVersionUID = -779488162448649143L;
	private static Logger logger = LoggerFactory
			.getLogger(TxNewsSpout.class);
	
	protected SpoutOutputCollector collector;
	private MonitorTools mt;
	//private DataFilterConf dfConf;
	private String splitKey;

	public TxNewsSpout(String config, ImmutableList<Output> outputField) {
		super(config, outputField);
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context, SpoutOutputCollector collector) {
		super.open(conf, context, collector);
		this.collector = collector;
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.splitKey = config.getString("tdbank_split_key", "\\|");
	}

	public void nextTuple() {
		if (messageConsumer != null) {
			Message message = null;
			try {
				message = messageQueue.poll(WAIT_FOR_NEXT_MESSAGE,
						TimeUnit.MILLISECONDS);
				if (message == null) {
					return;
				}
				TDMsg1 tdmsg = TDMsg1.parseFrom(message.getData());

				for (String attr : tdmsg.getAttrs()) {
					String categoryId = null;
					Map<String, String> map = Splitter.on("&").trimResults()
							.withKeyValueSeparator("=").split(attr);
					if (map.containsKey("iname")) {
						categoryId = map.get("iname");

						if(categoryId != null){
							Iterator<byte[]> it = tdmsg.getIterator(attr);
							while (it.hasNext()) {
								byte[] rawMessage = it.next();
								processMessage(rawMessage, categoryId);
							}
						}
					}
				}
			}catch (final InterruptedException e) {
				logger.error("", e);
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
	}
	
	public void processMessage(byte[] message,String categoryId){	
		int length = message.length;
		if (length <= 0) {
			logger.error("Msg message length is <0:");
			return ;
		} 

		if(this.mt!=null){
			MonitorEntry mEntryPut = new MonitorEntry(Constants.SUCCESSCODE,Constants.SUCCESSCODE);
			mEntryPut.addExtField("business_Name", Constants.bid);
			mEntryPut.addExtField("TDW_IDC", "tdbank");
			mEntryPut.addExtField("tbl_name", categoryId);
			this.mt.addCountEntry(Constants.systemID, Constants.tdbank_interfaceID, mEntryPut, 1);
		}
		
		String event;
		try {
			event = new String(message, "UTF-8");
			String[] event_array = event.split(splitKey,-1);		
			
			boolean isEmit = false;
			if(categoryId.equals(Constants.category_action) && event_array.length >= 16){
				isEmit = dealActionMsgByConfig(event_array);
			}else if (categoryId.equals(Constants.category_item) && event_array.length >= 17) {	
				isEmit = dealItemDetailMsgByConfig(event_array);
			}else if (categoryId.equals(Constants.category_user) && event_array.length >= 9) {	
				isEmit = dealUserDetailMsgByConfig(event_array);
			}else if (categoryId.equals(Constants.category_weight) && event_array.length >= 4) {	
				isEmit = dealActionWeightMsgByConfig(event_array);
			}else if(categoryId.equals(Constants.category_catelevel) && event_array.length >= 6){
				isEmit = dealCategoryLevelMsgByConfig(event_array);
			}
			
			
			if(!isEmit){
				this.collector.emit("error_data",new Values());
			}
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
	
	private String genHashKey(String qq,String uid){
		if(Utils.isQNumValid(qq)){
			return qq;
		}else if(!uid.equals("")){
			return uid;
		}else{
			return null;
		}
	}
		
	private boolean dealActionMsgByConfig(String[] msg_array){			
		String bid = msg_array[1];
		String qq = msg_array[2];
		String uid = msg_array[4];
		String imei = msg_array[5];
		String itemId = msg_array[6];
		String lbsInfo = msg_array[7];
		String platform = msg_array[8];
		String adpos = msg_array[9];
		String actType = msg_array[10];
		String actionTime = msg_array[12];
		String actionResult = msg_array[15];

		if(qq.equals("389687043") || qq.equals("475182144")){
			StringBuffer outputString = new StringBuffer(qq);		
			for(String each:msg_array){
				outputString.append(",").append(each);
			}		
			logger.info("-----"+outputString.toString());
		}
		
		String hashKey = genHashKey(qq,uid);
		if(hashKey == null){
			return false;
		}
		
		//is spcial for tx news
		if(actType.equals("1") || actType.equals("5") || actType.equals("9")
				||actType.equals("10") ||actType.equals("11") ||actType.equals("13")
				||actType.equals("25") || actType.equals("26")){
			
		}else if(actType.equals("16") || actType.equals("17") || actType.equals("18")){
			itemId = actionResult;
		}else{
			return false;
		}
		
		if(itemId.endsWith("#1") || itemId.endsWith("#2")){
			itemId = itemId.substring(0,itemId.length()-2);
		}

		if(!platform.equals("2") && !platform.equals("3")){
			return false;
		}
		
		if(!adpos.matches("[0-9]+")){
			return false;
		}
		
		if(!Utils.isActionTimeValid(actionTime)){
			return false;
		}
		
		if(!Utils.isRecommendAction(actType)){
			if(!Utils.isItemIdValid(itemId)){
				return false;
			}
		}

		Values outputValues = new Values();
		outputValues.add(hashKey);
		outputValues.add(bid);
		if(Utils.isRecommendAction(actType)){
			outputValues.add(Constants.recommend_action_stream);
		}else{
			outputValues.add(Constants.actions_stream);
		}
		outputValues.add(qq);
		outputValues.add(uid);
		outputValues.add(adpos);
		outputValues.add(actType);
		outputValues.add(actionTime);
		outputValues.add(itemId);
		outputValues.add(actionResult);
		outputValues.add(imei);
		outputValues.add(platform);
		outputValues.add(lbsInfo);

		
		if(Utils.isRecommendAction(actType)){
			if(qq.equals("389687043") || qq.equals("475182144")){
				logger.info("-----qq="+qq+",send to "+Constants.recommend_action_stream);
			}
			
			this.collector.emit(Constants.recommend_action_stream,outputValues);	
			if(this.mt!=null){
				MonitorEntry mEntryPut = new MonitorEntry(Constants.SUCCESSCODE,Constants.SUCCESSCODE);
				mEntryPut.addExtField("business_Name", Constants.bid);
				mEntryPut.addExtField("TDW_IDC", "tdprocess");
				mEntryPut.addExtField("tbl_name", Constants.recommend_action_stream);
				this.mt.addCountEntry(Constants.systemID, Constants.tdbank_interfaceID, mEntryPut, 1);
			}
		}else{
			
			if(qq.equals("389687043") || qq.equals("475182144")){
				logger.info("-----qq="+qq+",send to "+Constants.actions_stream);
			}
			
			this.collector.emit(Constants.actions_stream,outputValues);	
			if(this.mt!=null){
				MonitorEntry mEntryPut = new MonitorEntry(Constants.SUCCESSCODE,Constants.SUCCESSCODE);
				mEntryPut.addExtField("business_Name", Constants.bid);
				mEntryPut.addExtField("TDW_IDC", "tdprocess");
				mEntryPut.addExtField("tbl_name", Constants.actions_stream);
				this.mt.addCountEntry(Constants.systemID, Constants.tdbank_interfaceID, mEntryPut, 1);
			}
		}
		
		return true;
	}

	private boolean dealItemDetailMsgByConfig(String[] event_array){	
		String impDate = event_array[0];
		String bid = event_array[1];
		String itemId = event_array[2];
		String categoryId1 = event_array[3];
		String categoryId2 = event_array[4];
		String categoryId3 = event_array[5];
		String categoryName1 = event_array[6];
		String categoryName2 = event_array[7];
		String categoryName3 = event_array[8];
		String free = event_array[9];
		String publish = event_array[10];
		String price = event_array[11];
		
		String text = event_array[12];
		String itemTime = event_array[13];
		String expireTime = event_array[14];
		String platForm = event_array[15];
		String score = event_array[16];
		
		String shopId = "0";
				
		if(itemId.equals("") || !categoryId1.matches("[0-9]+") 
				|| categoryName1.equals("")){
			return false;
		}
		
		String[] dealMsg ={itemId,Constants.item_info_stream,bid,impDate,itemId,categoryId1,categoryId2,categoryId3,
					categoryName1,categoryName2,categoryName3,free,publish,price,text,itemTime,expireTime,platForm,score,shopId}; 

		Values outputValues = new Values();
		for(String value: dealMsg){
			outputValues.add(value);
		}
		this.collector.emit(Constants.item_info_stream,outputValues);
		if(this.mt!=null){
			MonitorEntry mEntryPut = new MonitorEntry(Constants.SUCCESSCODE,Constants.SUCCESSCODE);
			mEntryPut.addExtField("business_Name", Constants.bid);
			mEntryPut.addExtField("TDW_IDC", "tdprocess");
			mEntryPut.addExtField("tbl_name", Constants.item_info_stream);
			this.mt.addCountEntry(Constants.systemID, Constants.tdbank_interfaceID, mEntryPut, 1);
		}
		return true;
	}
	
	private boolean dealUserDetailMsgByConfig(String[] event_array){
		String impDate = event_array[0];
		String bid = event_array[1];
		String qq = event_array[2];
		String imei = event_array[4];
		String uid = event_array[5];
		String level = event_array[6];
		String regDate = event_array[7];
		String regTime = event_array[8];
		
		if(!Utils.isQNumValid(qq)){
			return false;
		}
		
		//hash_key,topic,bid,imp_date,qq,imei,uid,level,reg_date, reg_time
		String[] dealMsg ={qq,Constants.user_info_stream,bid,impDate,qq,imei,uid,level,regDate,regTime};
		
		Values outputValues = new Values();
		for(String value: dealMsg){
			outputValues.add(value);
		}
		this.collector.emit(Constants.user_info_stream,outputValues);	
		if(this.mt!=null){
			MonitorEntry mEntryPut = new MonitorEntry(Constants.SUCCESSCODE,Constants.SUCCESSCODE);
			mEntryPut.addExtField("business_Name", Constants.bid);
			mEntryPut.addExtField("TDW_IDC", "tdprocess");
			mEntryPut.addExtField("tbl_name", Constants.user_info_stream);
			this.mt.addCountEntry(Constants.systemID, Constants.tdbank_interfaceID, mEntryPut, 1);
		}
		return true;
	}
	
	private boolean dealActionWeightMsgByConfig(String[] event_array){
		String impDate = event_array[0];
		String bid = event_array[1];
		String actType = event_array[2];
		String weight = event_array[3];
		
		//hash_key,topic,bid,imp_date,type_id,weight
		String[] dealMsg ={actType,Constants.action_weight_stream,bid,impDate,actType,weight};
		
		Values outputValues = new Values();
		for(String value: dealMsg){
			outputValues.add(value);
		}
		this.collector.emit(Constants.action_weight_stream,outputValues);
		if(this.mt!=null){
			MonitorEntry mEntryPut = new MonitorEntry(Constants.SUCCESSCODE,Constants.SUCCESSCODE);
			mEntryPut.addExtField("business_Name", Constants.bid);
			mEntryPut.addExtField("TDW_IDC", "tdprocess");
			mEntryPut.addExtField("tbl_name", Constants.action_weight_stream);
			this.mt.addCountEntry(Constants.systemID, Constants.tdbank_interfaceID, mEntryPut, 1);
		}
		return true;
	}
	
	private boolean dealCategoryLevelMsgByConfig(String[] event_array){
		String impDate = event_array[0];
		String bid = event_array[1];
		String cateId = event_array[2];
		String cateName = event_array[3];
		String level = event_array[4];
		String fatherId = event_array[5];
		
		//hash_key,topic,bid,imp_date,cate_id,cate_name,level,father_id
		String[] dealMsg ={cateId,Constants.category_level_stream,bid,impDate,cateId,cateName,level,fatherId};
		
		Values outputValues = new Values();
		for(String value: dealMsg){
			outputValues.add(value);
		}
		this.collector.emit(Constants.category_level_stream,outputValues);
		if(this.mt!=null){
			MonitorEntry mEntryPut = new MonitorEntry(Constants.SUCCESSCODE,Constants.SUCCESSCODE);
			mEntryPut.addExtField("business_Name", Constants.bid);
			mEntryPut.addExtField("TDW_IDC", "tdprocess");
			mEntryPut.addExtField("tbl_name", Constants.category_level_stream);
			this.mt.addCountEntry(Constants.systemID, Constants.tdbank_interfaceID, mEntryPut, 1);
		}
		return true;
	}
	
	@Override
	public void processMessage(byte[] message) {
		// TODO Auto-generated method stub	
	}
	
}
