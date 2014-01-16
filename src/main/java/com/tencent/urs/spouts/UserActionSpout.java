package com.tencent.urs.spouts;

import java.util.Arrays;
import java.util.HashMap;
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

public class UserActionSpout extends TdbankSpout {
	
	private static final long serialVersionUID = -779488162448649143L;
	private static Logger logger = LoggerFactory
			.getLogger(UserActionSpout.class);
	public static byte SPEARATOR = (byte) 0xe0;

	protected SpoutOutputCollector collector;
	private MonitorTools mt;
	private DataFilterConf dfConf;
	private boolean debug;

	public UserActionSpout(String config, ImmutableList<Output> outputField) {
		super(config, outputField);
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context, SpoutOutputCollector collector) {
		super.open(conf, context, collector);
		this.collector = collector;
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.debug = Utils.getBoolean(conf, "debug", false);
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
			logger.error("check data failed,not found attr,message ="+ message.toString());
			return;
		}

		byte[] eventByte = Arrays.copyOfRange(message, 0, bodyIndex);
		byte[] attrByte = Arrays.copyOfRange(message, bodyIndex + 1, length - 1);

		String cate = new String(attrByte);
		String[] attrs = cate.split(",|:",-1);
		
		String categoryId = "";
		if (attrs.length >= 4) {
			categoryId = attrs[1];
		}
						
		String event = new String(eventByte);
		String[] event_array = event.split("\t",-1);

		if (categoryId.equals("yxpv") && event_array.length >= 30) {			
			String uin = event_array[2];
			String uid = event_array[3];
			String action_time = event_array[5];
			String itemId = event_array[7];
			String adpos = event_array[29];
			String action_type = "";
							
			if (event_array[22].indexOf("searchex.yixun.com") >= 0
					&& event_array[22].indexOf("searchex.yixun.com") < 15) {
				action_type = "1";
			} else if (event_array[22].indexOf("direct enter") != -1) {
				action_type = "2";
			} else if (event_array[22].indexOf("sale.yixun.com/morningmarket.html") != -1
					|| event_array[22].indexOf("sale.yixun.com/nightmarket.html") != -1
					|| event_array[22].indexOf("tuan.yixun.com") != -1) {
				action_type = "3";	
			} else if (event_array[22].indexOf("www.baidu.com") != -1) {
				action_type = "4";
			} else if (event_array[22].indexOf("event.yixun.com") != -1) {
				action_type = "5";
			} else{
				this.collector.emit("filter_data",new Values(""));
				return;			
			}
						
			//output=[1, UserAction, 20806747, 160821738, 4001, 1, 1389582548, 700919, , , , ]
			
			if(uin.endsWith("0") || uid.equals("0")){
				uin = "389687043";
				uid = "17139104";
			}
			String[] dealMsg ={"1","UserAction",uin,uid,adpos,action_type,action_time,itemId,"","","",""}; 
			dealMsgByConfig("1","UserAction",dealMsg);
		}else{
			this.collector.emit("filter_data",new Values(""));
		}
	}
	
	private void dealMsgByConfig(String bid,String topic,String[] msg_array){	
		if(msg_array == null){
			this.collector.emit("filter_data",new Values(""));
			return;	
		}
	
		Values outputValues = new Values();
		for(String value: msg_array){
			outputValues.add(value);
		}
		this.collector.emit(topic,outputValues);	
		if(outputValues.toString().indexOf("389687043")>=0){
			logger.info("log_output="+outputValues.toString());
		}		
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
