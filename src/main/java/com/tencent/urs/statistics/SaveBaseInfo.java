package com.tencent.urs.statistics;


import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Tuple;

import com.google.protobuf.InvalidProtocolBufferException;
import com.taobao.tair.client.Result;
import com.taobao.tair.client.TairClient.TairOption;
import com.taobao.tair.client.error.TairFlowLimit;
import com.taobao.tair.client.error.TairQueueOverflow;
import com.taobao.tair.client.error.TairRpcError;
import com.taobao.tair.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.monitor.MonitorEntry;
import com.tencent.monitor.MonitorTools;

import com.tencent.urs.algorithms.AlgAdpter;
import com.tencent.urs.asyncupdate.UpdateCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBackContext;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.LRUCache;

public class SaveBaseInfo implements AlgAdpter{
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private LRUCache<String, byte[]> combinerMap;
	private UpdateCallBack putCallBack;
	
	private static Logger logger = LoggerFactory
			.getLogger(SaveBaseInfo.class);
	
	@SuppressWarnings("rawtypes")
	public SaveBaseInfo(Map conf){
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.combinerMap = new LRUCache<String, byte[]>(50000);
		this.putCallBack = new UpdateCallBack(mt, Constants.systemID, Constants.tde_interfaceID, this.getClass().getName());
	}
	
	private void update(String key,byte[] values){		
		for(ClientAttr clientEntry:mtClientList ){
			TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, 12);
			try {
				Future<Result<Void>> future  = clientEntry.getClient().putAsync((short)12, key.getBytes(), values, putopt);
			
				clientEntry.getClient().notifyFuture(future, putCallBack, 
						new UpdateCallBackContext(clientEntry,key,values,putopt));
				synchronized(combinerMap){
					combinerMap.put(key, values);
				}
				
				if(mt!=null){
					MonitorEntry mEntryPut = new MonitorEntry(Constants.SUCCESSCODE,Constants.SUCCESSCODE);
					mEntryPut.addExtField("TDW_IDC", clientEntry.getGroupname());
					mEntryPut.addExtField("tbl_name", "FIFO1");
					mt.addCountEntry(Constants.systemID, Constants.tde_put_interfaceID, mEntryPut, 1);
				}
			} catch (Exception e){
				logger.error(e.toString());
			}
		}
	}

	@Override
	public void deal(Tuple input) {
		// TODO Auto-generated method stub
		String key = input.getStringByField("qq");
		byte[] newVal = null;
		update(key, newVal);	
	}
}