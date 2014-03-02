package com.tencent.urs.spouts;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.tencent.streaming.commons.spouts.tdbank.Output;
import com.tencent.streaming.commons.spouts.tdbank.TdbankSpout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import com.tencent.monitor.MonitorTools;
import com.tencent.urs.conf.DataFilterConf;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;

import backtype.storm.tuple.Values;

import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.Utils;

public class UrsInputSpout extends TdbankSpout {
	
	private static final long serialVersionUID = -779488162448649143L;
	private static Logger logger = LoggerFactory
			.getLogger(UrsInputSpout.class);
	public static byte SPEARATOR = (byte) 0xe0;

	protected SpoutOutputCollector collector;
	private MonitorTools mt;
	private DataFilterConf dfConf;
	private String topic;
	private HashSet<String> cateIDSet;

	public UrsInputSpout(String config, ImmutableList<Output> outputField) {
		super(config, outputField);
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context, SpoutOutputCollector collector) {
		super.open(conf, context, collector);
		this.collector = collector;
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.cateIDSet = new HashSet<String>();
		this.topic = Constants.actions_stream;
	}

	@Override
	public void processMessage(byte[] message){	
		int length = message.length;
		if (length <= 0) {
			logger.error("Msg message length is <0:");
			return ;
		} 

		int bodyIndex = searchIndex(message, SPEARATOR);
		if (bodyIndex == -1 || bodyIndex == length - 1) {
			logger.error("check data failed,not found attr");
			return;
		}

		byte[] eventByte = Arrays.copyOfRange(message, 0, bodyIndex);
		byte[] attrByte = Arrays.copyOfRange(message, bodyIndex + 1, length);

		String cate = new String(attrByte);
		String[] attrs = cate.split(",|:",-1);
		
		String categoryId = "";
		if (attrs.length >= 4) {
			categoryId = attrs[1];
		}
		
		String event = new String(eventByte);
		String[] event_array = event.split(",",-1);		
		
		if(categoryId.equals("pppv")){
			String[] itemDetailMsg = genItemDetailMsg(categoryId,event);
			if(itemDetailMsg != null && itemDetailMsg.length >= 17){
				dealItemDetailMsgByConfig(itemDetailMsg);
			}
		}
		
		if(categoryId.equals("pppv") || categoryId.equals("ppclick") 
				|| categoryId.equals("commoditypv") || categoryId.equals("ppdeal")){
			event_array = genPPMsg(categoryId,event);
			if(event_array == null || event_array.length <= 15){
				this.collector.emit(categoryId,new Values(""));
				return;
			}
			categoryId = Constants.actions_stream;
		}
		
		if(categoryId.equals(Constants.actions_stream) && event_array.length >= 16){
			dealActionMsgByConfig(event_array);
		}else if (categoryId.equals(Constants.item_info_stream) && event_array.length >= 17) {	
			dealItemDetailMsgByConfig(event_array);
		}else if (categoryId.equals(Constants.user_info_stream) && event_array.length >= 9) {	
			dealUserDetailMsgByConfig(event_array);
		}else if (categoryId.equals(Constants.action_weight_stream) && event_array.length >= 4) {	
			dealActionWeightMsgByConfig(event_array);
		}else if(categoryId.equals(Constants.category_level_stream) && event_array.length >= 6){
			dealCategoryLevelMsgByConfig(event_array);
		}else{
			this.collector.emit(categoryId,new Values(""));
		}
	}
	
	private String genHashKey(String qq,String uid){
		if(Utils.isQNumValid(qq)){
			return qq;
		}else if(Utils.isQNumValid(uid)){
			return uid;
		}else{
			return null;
		}
	}
		
	private String[] genItemDetailMsg(String categoryId, String event){
		String impDate = "0";
		String bid = "2";
		String itemId = "0";
		String shopId = "0";
		String categoryId1 = "0";
		String categoryId2 = "0";
		String categoryId3 = "0";
		String categoryName1 = "";
		String categoryName2 = "";
		String categoryName3 = "";
		String free = "0";
		String publish = "0";
		String price = "0";
		
		String text = "0";
		String itemTime = "0";
		String expireTime = "0";
		String platForm = "0";
		String score = "0";
				
		String[] event_array = event.split("\t",-1);
		if (categoryId.equals("pppv") && event_array.length > 15) {	
			Long time = Long.valueOf(event_array[4])*1000L;
			impDate = new SimpleDateFormat("yyyyMMdd").format(time);
			shopId = event_array[7];
			itemId = event_array[6];
			categoryId1 = event_array[8];
			categoryId3 = event_array[9];
		}else{
			return null;
		}
		
		if(shopId.equals("0") || itemId.equals("0") || impDate.equals("0"))
		{
			return null;
		}
		
		String[] dealMsg ={itemId,Constants.item_info_stream,bid,impDate,itemId,categoryId1,categoryId2,categoryId3,
				categoryName1,categoryName2,categoryName3,free,publish,price,text,itemTime,expireTime,platForm,score,shopId}; 
		return dealMsg;
	}
	
	private String[] genPPMsg(String categoryId, String event){
		String impDate = "";
		String bid = "10070002";
		String weixin_no = "";
		String qq =  "0";
		String actionDate = "0";
		String actionTime = "0";
		String uid = "0";
		String adpos = "0";
		String actType = "0";
		String itemId = "0";
		String actionResult = "";
		String imei = "";
		String platform = "";
		String lbsInfo = "";
		
		String expId = "";
		String error = "";
				
		String[] event_array = event.split("\t",-1);
		if (categoryId.equals("pppv") && event_array.length > 15) {	
			qq =  event_array[2];
			actionTime = event_array[4];
			actType = "3";
			itemId = event_array[6];
		}else if (categoryId.equals("ppclick") && event_array.length > 16) {	
			qq =  event_array[2];
			actionTime = event_array[4];
			adpos = event_array[8];
			actType = "2";
			itemId = event_array[6];
			actionResult = event_array[16];
		}else if (categoryId.equals("commoditypv") && event_array.length > 4) {	
			qq =  event_array[2];
			actionTime = event_array[3];
			adpos = event_array[0];
			actType = "1";

			actionResult = event_array[4];
			for(int i = 5; i < event_array.length; i++){
				actionResult = actionResult + ";" +event_array[i];
			}
		}else if(categoryId.equals("ppdeal") && event_array.length > 1){
			actionTime = event_array[0];
			qq =  event_array[2];		
			actType = "7";
			itemId = event_array[7];
		}else{
			return null;
		}
		
		if(adpos.length() > 2  || adpos.equals("") || !adpos.matches("[0-9]+")){
			return null;
		}

		String[] returnstr = {impDate,bid,qq, weixin_no,uid,imei,itemId,lbsInfo,platform,adpos,actType,actionDate,actionTime,error,expId,actionResult};
		return  returnstr;
	}
	
	private void dealActionMsgByConfig(String[] msg_array){			
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

		String hashKey = genHashKey(qq,uid);
		if(hashKey == null){
			this.collector.emit("error_data",new Values());
			return;
		}
		
		Recommend.ActiveType actTypeValue = Utils.getActionTypeByString(actType);
		
		if(actTypeValue != Recommend.ActiveType.Impress 
				&& actTypeValue != Recommend.ActiveType.Click){
			if(!Utils.isItemIdValid(itemId)){
				this.collector.emit("error_data",new Values());
				return;
			}
		}

		Values outputValues = new Values();
		outputValues.add(hashKey);
		outputValues.add(bid);
		outputValues.add(topic);
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

		this.collector.emit(Constants.actions_stream,outputValues);	
	}

	private void dealItemDetailMsgByConfig(String[] event_array){	
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
		
		String shopId = event_array[17];
				

		String[] dealMsg ={itemId,Constants.item_info_stream,bid,impDate,itemId,categoryId1,categoryId2,categoryId3,
					categoryName1,categoryName2,categoryName3,free,publish,price,text,itemTime,expireTime,platForm,score,shopId}; 

		Values outputValues = new Values();
		for(String value: dealMsg){
			outputValues.add(value);
		}
		this.collector.emit(Constants.item_info_stream,outputValues);
	}
	
	private void dealUserDetailMsgByConfig(String[] event_array){
		String impDate = event_array[0];
		String bid = event_array[1];
		String qq = event_array[2];
		String imei = event_array[4];
		String uid = event_array[5];
		String level = event_array[6];
		String regDate = event_array[7];
		String regTime = event_array[8];
				
		//hash_key,topic,bid,imp_date,qq,imei,uid,level,reg_date, reg_time
		String[] dealMsg ={qq,Constants.user_info_stream,bid,impDate,qq,imei,uid,level,regDate,regTime};
		
		Values outputValues = new Values();
		for(String value: dealMsg){
			outputValues.add(value);
		}
		this.collector.emit(Constants.user_info_stream,outputValues);				
	}
	
	private void dealActionWeightMsgByConfig(String[] event_array){
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
		this.collector.emit(Constants.user_info_stream,outputValues);	
	}
	
	private void dealCategoryLevelMsgByConfig(String[] event_array){
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
	}
	
	private int searchIndex(byte[] bytes, byte key) {
		int length = bytes.length;
		for (int i = length - 1; i >= 0; i--) {
			if (bytes[i] == key) {
				return i;
			}
		}
		return -1;
	}
	
}
