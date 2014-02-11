package com.tencent.urs.spouts;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;

import com.tencent.monitor.MonitorTools;
import com.tencent.streaming.commons.spouts.tdbank.Output;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.utils.Constants;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Time;


/**
 * @author stevenxiang
 * 
 */

@SuppressWarnings("serial")
public class TestSpout2 implements IRichSpout {
	private static Logger logger = LoggerFactory
			.getLogger(TestSpout2.class);

	protected SpoutOutputCollector collector;

	public TestSpout2(String config, ImmutableList<Output> outputField) {
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.spout.ISpout#open(java.util.Map,
	 * backtype.storm.task.TopologyContext,
	 * backtype.storm.spout.SpoutOutputCollector)
	 */
	public void open(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context, SpoutOutputCollector collector) {
		try {
			this.collector = collector;		
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.spout.ISpout#nextTuple()
	 */
	public void nextTuple() {
		Long now = System.currentTimeMillis()/1000L;
		String itemId = String.valueOf(now.intValue()%30);
		String actType = String.valueOf(now%10+1);
		//<fields>bid,topic,qq,uid,adpos,action_type,action_time,item_id,action_result,imei,platform,lbs_info</fields>
		String action_result = itemId+"0";
		for(int i=0; i<10; i++){
			action_result = action_result +";"+ itemId+i;
		}
		
		String[] dealMsg ={"1","user_action","0","17139104","1",actType,String.valueOf(now),itemId,action_result,"","",""}; 
		String[] dealMsg2 ={"1","user_action","0","17139104","1","1",String.valueOf(now),itemId,action_result,"","",""}; 
		String[] dealMsg3 ={"1","user_action","0","17139104","1","2",String.valueOf(now),itemId,action_result,"","",""}; 
		dealMsgByConfig("1","user_action",dealMsg);
		dealMsgByConfig("1","user_action",dealMsg2);
		dealMsgByConfig("1","user_action",dealMsg3);
		try {
			Time.sleep(1000L);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			logger.error(e.toString());
		}

	}
	
	private void dealMsgByConfig(String bid,String topic,String[] msg_array){	
		Values outputValues = new Values();
		for(String value: msg_array){
			outputValues.add(value);
		}
		this.collector.emit(topic,outputValues);			
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.spout.ISpout#close()
	 */
	public void close() {
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.spout.ISpout#deactivate()
	 */
	public void deactivate() {
		// TODO Auto-generated method stub
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.topology.IComponent#getComponentConfiguration()
	 */
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public void activate() {
		// TODO Auto-generated method stub
	}

	public void ack(Object msgId) {
		// TODO Auto-generated method stub
	}

	public void fail(Object msgId) {
		// TODO Auto-generated method stub
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//bid,topic,qq,uid,adpos,action_type,action_time,item_id,action_result,imei,platform,lbs_info
		declarer.declareStream(Constants.actions_stream, 
					new Fields("bid","topic","qq","uid","adpos","action_type","action_time","item_id","action_result","imei","platform","lbs_info"));
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
	}
}
