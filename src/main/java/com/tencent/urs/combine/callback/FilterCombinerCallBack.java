package com.tencent.urs.combine.callback;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.taobao.tair.client.TairClient.TairOption;

import com.taobao.tair.client.impl.MutiThreadCallbackClient;
import com.tencent.monitor.MonitorTools;
import com.tencent.urs.combine.CombineKey;
import com.tencent.urs.combine.CombineValue;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;

import backtype.storm.task.OutputCollector;
import backtype.storm.utils.TimeCacheMap.ExpiredCallback;

public class FilterCombinerCallBack implements ExpiredCallback {
	private static Logger logger = LoggerFactory
			.getLogger(FilterCombinerCallBack.class);

	private List<ClientAttr> mtClientList;
	private MutiThreadCallbackClient client;
	private TairOption opt;
	//private UpdateCallBack putCallBack;
	private MonitorTools mt;

	public FilterCombinerCallBack(@SuppressWarnings("rawtypes") Map conf,
			OutputCollector collector) {
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		//this.putCallBack = new UpdateCallBack(mt, Constants.systemID,Constants.tde_interfaceID, "FilterBolt");
		
		this.client = mtClientList.get(0).getClient();
		this.opt = new TairOption(mtClientList.get(0).getTimeout());		
	}

	@Override
	public void expire(Object key, Object val) {
		// TODO Auto-generated method stub
		update((CombineKey)key, (CombineValue)val);
	}
	
	private void update(CombineKey key,CombineValue value){
		
	}
}
