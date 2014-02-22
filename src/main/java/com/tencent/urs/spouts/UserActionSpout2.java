package com.tencent.urs.spouts;

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
import com.tencent.urs.utils.Utils;

public class UserActionSpout2 extends TdbankSpout {
	
	private static final long serialVersionUID = -779488162448649143L;
	private static Logger logger = LoggerFactory
			.getLogger(UserActionSpout2.class);
	public static byte SPEARATOR = (byte) 0xe0;

	protected SpoutOutputCollector collector;
	private MonitorTools mt;
	private DataFilterConf dfConf;
	private String topic;
	private HashSet<String> cateIDSet;

	public UserActionSpout2(String config, ImmutableList<Output> outputField) {
		super(config, outputField);
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context, SpoutOutputCollector collector) {
		super.open(conf, context, collector);
		this.collector = collector;
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.cateIDSet = new HashSet<String>();
		this.topic = "user_action";
	}

	@Override
	public void processMessage(byte[] message){	
		int length = message.length;
		if (length <= 0) {
			logger.info("Msg message length is <0:");
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
		
		String[] dealMsg = null;
		if(true){
			dealMsg = genPPMsg(categoryId,event);
		}else{
			dealMsg = event.split(",",-1);
		}
		
				
		if(dealMsg != null && dealMsg.length >= 16){
			dealMsgByConfig(dealMsg);
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
	
	private String[] genPPMsg(String categoryId, String event){
		String impDate = "";
		String bid = "2";
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
			adpos = event_array[15];
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
		}else if(categoryId.equals("ppdeal") && event_array.length > 16){
			logger.info("ppdeal:"+event);
			qq =  event_array[4];
			actionTime = event_array[0];
			adpos = event_array[8];
			actType = "7";
			itemId = event_array[6];
			actionResult = event_array[16];
		}else{
			return null;
		}
		
		if(adpos.length() > 2  || adpos.equals("") || !adpos.matches("[0-9]+")){
			return null;
		}

		String[] returnstr = {impDate,bid,qq, weixin_no,uid,imei,itemId,lbsInfo,platform,adpos,actType,actionDate,actionTime,error,expId,actionResult};
		return  returnstr;
	}
	
	private void dealMsgByConfig(String[] msg_array){			
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
		
		if(!actType.equals("1") && Utils.isItemIdValid(itemId)){
			this.collector.emit("error_data",new Values());
			return;
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
		
		this.collector.emit("user_action",outputValues);	
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
