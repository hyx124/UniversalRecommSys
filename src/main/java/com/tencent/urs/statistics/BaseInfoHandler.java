package com.tencent.urs.statistics;

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
import com.tencent.urs.conf.AlgModuleConf.AlgModuleInfo;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.Utils;

public class BaseInfoHandler implements AlgAdpter{
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private UpdateCallBack putCallBack;
	private ConcurrentHashMap<String, byte[]> combinerMap;
	private int nsTableID;
	
	private static Logger logger = LoggerFactory
			.getLogger(BaseInfoHandler.class);
	
	@SuppressWarnings("rawtypes")
	public BaseInfoHandler(Map conf,AlgModuleInfo algInfo){
		this.nsTableID = Utils.getInt(conf, "tableid", 11);
		
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_interfaceID, this.getClass().getName());
			
		this.combinerMap = new ConcurrentHashMap<String,byte[]>(1024);
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
							byte[] expireTimeValue  = combinerMap.remove(key);
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
	
	//基础属性的value不需要合并，直接覆盖
	private void combinerKeys(String key,byte[] value) {
		combinerMap.put(key,value);
	}	
	
	private void update(String key,byte[] values) 
			throws TairRpcError, TairFlowLimit, TairQueueOverflow{	
		for(ClientAttr clientEntry:mtClientList){
			TairOption opt = new TairOption(clientEntry.getTimeout());
			Future<Result<Void>> future = clientEntry.getClient().putAsync((short)nsTableID, key.toString().getBytes(), values.toString().getBytes(), opt);	
			clientEntry.getClient().notifyFuture(future, putCallBack, new UpdateCallBackContext(clientEntry,key.toString(),values.toString().getBytes(),opt));
		}
	}

	private byte[] genPbBytes(Tuple input) {
		return null;
	}
	
	private String genKey(Tuple input) {
		String topic = input.getStringByField("topic");
		String bid = input.getStringByField("bid");
		String key = null;
		if(topic.equals("")){
			key =  bid+"#"+input.getStringByField("qq")+"#algName";
		}else if(topic.equals("")){
			key =  bid+"#"+input.getStringByField("item")+"#algName";
		}else{
			key = null;
		}
		
		
		return key;
	}
	
	@Override
	public void deal(AlgModuleInfo algInfo,Tuple input) {
		// TODO Auto-generated method stub
		String key = genKey(input);			
		byte[] value = genPbBytes(input);
		
		if(key!=null && value!=null){
			combinerKeys(key,value);
		}
				
	}

}