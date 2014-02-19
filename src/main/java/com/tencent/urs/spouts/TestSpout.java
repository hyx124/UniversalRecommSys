package com.tencent.urs.spouts;

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

public class TestSpout extends TdbankSpout {
	
	private static final long serialVersionUID = -779488162448649143L;
	private static Logger logger = LoggerFactory
			.getLogger(TestSpout.class);
	public static byte SPEARATOR = (byte) 0xe0;

	protected SpoutOutputCollector collector;
	private MonitorTools mt;
	private DataFilterConf dfConf;
	private boolean debug;
	private int count;

	public TestSpout(String config, ImmutableList<Output> outputField) {
		super(config, outputField);
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context, SpoutOutputCollector collector) {
		super.open(conf, context, collector);
		this.collector = collector;
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.debug = Utils.getBoolean(conf, "debug", false);
		this.count = 1;
	}

	@Override
	public void processMessage(byte[] message){	
		Long now = System.currentTimeMillis()/1000L;
		String itemId = String.valueOf(now%30+1);
		String actType = String.valueOf(now%10);
		//output=[1, UserAction, 20806747, 160821738, 4001, 1, 1389582548, 700919, , , , ]
		String[] dealMsg ={"1","user_action","0","17139104","1",actType,String.valueOf(now),itemId,"","","",""}; 
		if(count %1000 == 0){
			dealMsgByConfig("1","user_action",dealMsg);
			count = 0;
		}else{
			this.collector.emit("filter_data",new Values(""));
		}
		count ++;
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
	}
	
}
