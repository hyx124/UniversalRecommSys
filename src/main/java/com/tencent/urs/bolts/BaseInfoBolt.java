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
import com.tencent.urs.conf.AlgModuleConf;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.Utils;

public class BaseInfoBolt extends AbstractConfigUpdateBolt{
	private static final long serialVersionUID = -1302335947421120663L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private UpdateCallBack putCallBack;
	private ConcurrentHashMap<String, byte[]> combinerMap;
	private int nsTableID;
	private AlgModuleConf algInfo;
	
	private static Logger logger = LoggerFactory
			.getLogger(BaseInfoBolt.class);
	
	@SuppressWarnings("rawtypes")
	public BaseInfoBolt(String config, ImmutableList<Output> outputField,
			String sid){
		super(config, outputField, sid);
		this.updateConfig(super.config);		
	}
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		this.nsTableID = Utils.getInt(conf, "tableid", 11);
		
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_interfaceID, this.getClass().getName());
			
		this.combinerMap = new ConcurrentHashMap<String,byte[]>(1024);
		int expireTime = Utils.getInt(conf, "expireTime",5*3600);
		setCombinerTime(expireTime);
	}

	
	private void setCombinerTime(final int second) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					while (true) {
						Thread.sleep(second * 1000);
						Set<String> keySet = combinerMap.keySet();
						for (String key : keySet) {
							byte[] expireTimeValue  = combinerMap.remove(key);
							try{
								Save(key,expireTimeValue);
							}catch(Exception e){
								//mt.addCountEntry(systemID, interfaceID, item, count)
							}
						}
					}
				} catch (Exception e) {
					logger.error("Schedule thread error:" + e, e);
				}
			}
		}).start();
	}
	
	//基础属性的value不需要合并，直接覆盖
	private void combinerKeys(String key,byte[] value) {
		combinerMap.put(key,value);
	}	
	
	private void Save(String key,byte[] values) 
			throws TairRpcError, TairFlowLimit, TairQueueOverflow{	
		for(ClientAttr clientEntry:mtClientList){
			TairOption opt = new TairOption(clientEntry.getTimeout());
			Future<Result<Void>> future = clientEntry.getClient().putAsync((short)nsTableID, 
								key.toString().getBytes(), values.toString().getBytes(), opt);	
			clientEntry.getClient().notifyFuture(future, putCallBack, 
					new UpdateCallBackContext(clientEntry,key.toString(),values.toString().getBytes(),opt));
		}
	}

	private byte[] genPbBytes(String bid,Tuple input) {
		return null;
	}
	
	private String genKey(String sid,Tuple input) {
		String bid = input.getStringByField("bid");
		String key="";
		if(sid.equals("")){
			key =  bid+"#"+input.getStringByField("qq")+"#"+algInfo.getAlgName();
		}else if(sid.equals("")){
			key =  bid+"#"+input.getStringByField("item_id")+"#"+algInfo.getAlgName();
		}else{
			key = null;
		}
		return key;
	}

	@Override
	public void updateConfig(XMLConfiguration config) {
		try {
			this.algInfo.load(config);
		} catch (ConfigurationException e) {
			logger.error(e.toString());
		}
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {
		String key = genKey(sid,tuple);			
		byte[] value = genPbBytes(sid,tuple);
		
		if(key!=null && value!=null){
			combinerKeys(key,value);
		}
	}

}