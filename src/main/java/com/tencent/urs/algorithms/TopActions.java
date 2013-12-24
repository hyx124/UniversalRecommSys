package com.tencent.urs.algorithms;


import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.protobuf.InvalidProtocolBufferException;
import com.tencent.monitor.MonitorTools;

import com.tencent.tde.client.Result;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.error.TairFlowLimit;
import com.tencent.tde.client.error.TairQueueOverflow;
import com.tencent.tde.client.error.TairRpcError;
import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.algorithms.AlgAdpter;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.LRUCache;

public class TopActions implements AlgAdpter{
	private int topNum;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private LRUCache<String, byte[]> combinerMap;
	private OutputCollector collector;
	
	private static Logger logger = LoggerFactory
			.getLogger(TopActions.class);
	
	@SuppressWarnings("rawtypes")
	public TopActions(Map conf,OutputCollector collector,int topNum){
		this.topNum = topNum;
		this.collector = collector;
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.combinerMap = new LRUCache<String, byte[]>(50000);

	}

	private class TopActionsUpdateAysncCallback implements MutiClientCallBack{
		private final String key;
		private final byte[] values;

		public TopActionsUpdateAysncCallback(String key, byte[] values) {
			this.key = key ; 
			this.values = values;								
		}

		public void excute() {
			try {
				if(combinerMap.containsKey(key)){		
					byte[] oldVal = combinerMap.get(key);	
					byte[] mergeVal = mergeToHeap(values,oldVal);
					savAndEmit(key,mergeVal);
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)12,key.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}			
				
			} catch (TairQueueOverflow e) {
				logger.error("TairQueueOverflow:"+e.toString());
			} catch (TairRpcError e) {
				logger.error("TairRpcError:"+e.toString());
			} catch (TairFlowLimit e) {
				logger.error("TairFlowLimit:"+e.toString());
			} catch (InvalidProtocolBufferException e) {
				logger.error("InvalidProtocolBufferException:"+e.toString());
			}
		}

		private void savAndEmit(String key,byte[] values){		
			combinerMap.put(key, values);
			collector.emit("algModuleName",new Values(key,values));
		}
		
		private void insertToHeapBuilder(Recommend.UserActiveHistory oldHeap ,
						Recommend.UserActiveHistory.Builder newHeapBuilder,
						Recommend.UserActiveHistory.ActiveRecord newVal){
			
			HashSet<String> alreadyIn = new HashSet<String>();
			for(Recommend.UserActiveHistory.ActiveRecord oldVal:oldHeap.getActRecordsList()){
				if(newHeapBuilder.getActRecordsCount() >= topNum){
					break;
				}
			
				//step1,判断是否已经插入了,
				//step2,如果相同，merge两个值的内容后插入
				//step3，如果不相同，先放大的，后仿小的
				
				if(!alreadyIn.contains(newVal.getItem())){
					if(oldVal.getItem().equals(newVal.getItem())){
						newHeapBuilder.addActRecords(newVal);
						alreadyIn.add(newVal.getItem());
					}else{
						//先放大的，再放小的
						if(oldVal.getActTime() <= newVal.getActTime()){
							newHeapBuilder.addActRecords(newVal);
							alreadyIn.add(newVal.getItem());											
							
							newHeapBuilder.addActRecords(oldVal);
							alreadyIn.add(oldVal.getItem());
						}else{
							newHeapBuilder.addActRecords(oldVal);
							alreadyIn.add(oldVal.getItem());
						}	
					}
				}else if(!alreadyIn.contains(oldVal.getItem())){
					newHeapBuilder.addActRecords(oldVal);
					alreadyIn.add(oldVal.getItem());
				}
			}
			
			if(!alreadyIn.contains(newVal.getItem()) && newHeapBuilder.getActRecordsCount() < topNum){
				newHeapBuilder.addActRecords(newVal);
			}

		}

		private byte[] mergeToHeap(byte[] newVal,byte[] oldVal) throws InvalidProtocolBufferException{
			Recommend.UserActiveHistory.Builder newHeapBuilder = Recommend.UserActiveHistory.newBuilder();
			Recommend.UserActiveHistory oldHeap = Recommend.UserActiveHistory.parseFrom(oldVal);
			insertToHeapBuilder(oldHeap,newHeapBuilder,Recommend.UserActiveHistory.ActiveRecord.parseFrom(newVal));
			return newHeapBuilder.build().toByteArray();
			
		}
		

		@Override
		public void handle(Future<?> future, Object context) {			
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			byte[] oldValue = null;
			try {
				oldValue = afuture.get().getResult();
				byte[] mergeValue = mergeToHeap(values,oldValue);
				savAndEmit(key,mergeValue);
			} catch (Exception e) {
				
			}
			
		}
	}

	@Override
	public void deal(Tuple input) {
		// TODO Auto-generated method stub
		String key = input.getStringByField("qq");
		byte[] newVal = null;
		new TopActionsUpdateAysncCallback(key, newVal).excute();	
	}
}