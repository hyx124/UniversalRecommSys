package com.tencent.urs.process;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.urs.algorithms.AlgAdpter;
import com.tencent.urs.statistics.SaveBaseInfo;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * @author root
 * 
 */
public class AlgDealBolt implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4509340927279154699L;
	// private CtrProfile profile;
	private String algName;
	private OutputCollector collector;
	private AlgAdpter algAdpter;
	private static Logger logger = LoggerFactory
			.getLogger(AlgDealBolt.class);
	
	public AlgDealBolt(String algName){
		this.algName = algName;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map,
	 * backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		if(algName.equals("SaveBaseInfo")){
			this.algAdpter = new SaveBaseInfo(stormConf);
		}		
	}

/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		try{		
			algAdpter.deal(input);
		}
		catch(Exception e){
			logger.error(e.toString());
		}
		
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.task.IBolt#cleanup()
	 */
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * backtype.storm.topology.IComponent#declareOutputFields(backtype.storm
	 * .topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("", new Fields("tag","tabId","uin","group_id","cid","type","logtime","refer","price","siteId"));
		declarer.declareStream("", new Fields("tag","tabId","uin","group_id","cid","type","logtime","refer","price","siteId"));
		
		declarer.declareStream("error_data", new Fields("hashkey"));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.topology.IComponent#getComponentConfiguration()
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
	}

}
