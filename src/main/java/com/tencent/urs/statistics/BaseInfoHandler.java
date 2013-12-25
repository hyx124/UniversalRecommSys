package com.tencent.urs.statistics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;

import com.tencent.monitor.MonitorTools;
import com.tencent.tde.client.Result;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.error.TairFlowLimit;
import com.tencent.tde.client.error.TairQueueOverflow;
import com.tencent.tde.client.error.TairRpcError;

import com.tencent.urs.algorithms.AlgAdpter;
import com.tencent.urs.asyncupdate.UpdateCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBackContext;
import com.tencent.urs.combine.UpdateKey;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.Utils;

public class BaseInfoHandler implements AlgAdpter{
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private UpdateCallBack putCallBack;
	private ConcurrentHashMap<String, ArrayList<String>> combinerMap;
	private int nsTableID;
	
	private static Logger logger = LoggerFactory
			.getLogger(BaseInfoHandler.class);
	
	@SuppressWarnings("rawtypes")
	public BaseInfoHandler(Map conf){
		this.nsTableID = Utils.getInt(conf, "tableid", 11);
		
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_interfaceID, this.getClass().getName());
			
		this.combinerMap = new ConcurrentHashMap<String,ArrayList<String>>(1024);
		int expireTime = Utils.getInt(conf, "expireTime",5*3600);
		setCombinerTime(expireTime, this);
		
	}
	
	private void setCombinerTime(final int second, final AlgAdpter bolt) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					while (true) {
						Thread.sleep(second * 1000);
						Set<String> keySet = combinerMap.keySet();
						for (String key : keySet) {
							ArrayList<String> expireTimeValue  = combinerMap.remove(key);
							try{
								update(key,expireTimeValue);
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
	
	private void combinerKeys(String key,ArrayList<String> value) {
		combinerMap.put(key,value);
	}	
	
	private void update(String key,ArrayList<String> values) 
			throws TairRpcError, TairFlowLimit, TairQueueOverflow{	
		for(ClientAttr clientEntry:mtClientList){
			TairOption opt = new TairOption(clientEntry.getTimeout());
			Future<Result<Void>> future = clientEntry.getClient().putAsync((short)nsTableID, key.toString().getBytes(), values.toString().getBytes(), opt);	
			clientEntry.getClient().notifyFuture(future, putCallBack, new UpdateCallBackContext(clientEntry,key.toString(),values.toString().getBytes(),opt));
		}
	}

	@Override
	public void deal(Tuple input) {
		// TODO Auto-generated method stub
		String topic = input.getStringByField("topic");
		String key=null;
		ArrayList<String> values = new ArrayList<String>();
		if(topic.equals("user_info")){
			String qq = input.getStringByField("qq");
			String item_id = input.getStringByField("item_id");
			String attr = input.getStringByField("attr");
			values.add(attr);
			
		}else if(topic.equals("item_info")){
			//key = item_id;
			
		}else if(topic.equals("item_category_info")){
			//key = item_id;
			
			
		}else{
			return;
		}
		
		if(key!=null){
			combinerKeys(key,values);
		}
		
	}
}