package com.tencent.urs.spouts;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import com.tencent.streaming.commons.spouts.tdbank.Output;
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
		String itemId = String.valueOf(now.intValue()%1000);
		String actType = String.valueOf(now%10+1);
		//<fields>bid,topic,qq,uid,adpos,action_type,action_time,item_id,action_result,imei,platform,lbs_info</fields>
		String action_result = itemId+"0";
		for(int i=1; i<10; i++){
			action_result = action_result +";"+ itemId+i;
		}
		
		String[] dealMsg1 ={"17139104","1","user_action","0","17139104","1","1",String.valueOf(now),itemId,action_result,"","",""}; 
		String[] dealMsg2 ={"17139104","1","user_action","0","17139104","1","2",String.valueOf(now),itemId,action_result,"","",""}; 
		String[] dealMsg3 ={"17139104","1","user_action","0","17139104","1","3",String.valueOf(now),itemId,action_result,"","",""}; 
		String[] dealMsg4 ={"17139104","1","user_action","0","17139104","1","7",String.valueOf(now),itemId,action_result,"","",""}; 
		dealMsgByConfig("1","user_action",dealMsg1);
		dealMsgByConfig("1","user_action",dealMsg2);
		dealMsgByConfig("1","user_action",dealMsg3);
		dealMsgByConfig("1","user_action",dealMsg4);
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
					new Fields("hash_key","bid","topic","qq","uid","adpos","action_type","action_time","item_id","action_result","imei","platform","lbs_info"));

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
	}
}
