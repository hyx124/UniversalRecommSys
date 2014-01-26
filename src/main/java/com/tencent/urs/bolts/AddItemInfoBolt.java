package com.tencent.urs.bolts;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.tencent.monitor.MonitorTools;
import com.tencent.streaming.commons.bolts.config.AbstractConfigUpdateBolt;
import com.tencent.streaming.commons.spouts.tdbank.Output;
import com.tencent.tde.client.Result;
import com.tencent.tde.client.Result.ResultCode;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.error.TairFlowLimit;
import com.tencent.tde.client.error.TairRpcError;
import com.tencent.tde.client.error.TairTimeout;
import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.combine.ActionCombinerValue;
import com.tencent.urs.combine.UpdateKey;
import com.tencent.urs.conf.DataFilterConf;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.Utils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class AddItemInfoBolt extends AbstractConfigUpdateBolt {
	private static final long serialVersionUID = 1L;
	private List<ClientAttr> mtClientList;	
	private DataCache<String> qqCache;
	private DataCache<String> groupIdCache;
	private OutputCollector collector;
	
	private int cacheExpireTime;
	private int nsTableGroup;
	private int nsTableUin;
	
	private static Logger logger = LoggerFactory
			.getLogger(AddItemInfoBolt.class);

	public AddItemInfoBolt(String config, ImmutableList<Output> outputField){
		super(config, outputField, Constants.config_stream);
	}
	
	@Override 
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		super.prepare(conf, context, collector);
		this.collector = collector;
		this.qqCache = new DataCache<String>(conf);
		this.groupIdCache = new DataCache<String>(conf);
		this.collector = collector;
		this.cacheExpireTime = 1*24*3600;
		this.nsTableGroup = Utils.getInt(conf, "tdengine.table.indival", 319);
		this.nsTableUin = Utils.getInt(conf, "tdengine.table.indival", 320);
		
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		ClientAttr clientEntry = mtClientList.get(0);		
		TairOption opt = new TairOption(clientEntry.getTimeout(),(short)0, 24*3600);
	
	} 
	
	@Override
	public void updateConfig(XMLConfiguration config) {
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {
		String topic = tuple.getStringByField("topic");	
		String qq = tuple.getStringByField("qq");
		String uid = tuple.getStringByField("uid");	

		Values outputValues = new Values();
	}
	
}