package com.tencent.urs.utils;

import java.lang.ref.SoftReference;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;

import com.tencent.tde.client.Result;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.error.TairFlowLimit;
import com.tencent.tde.client.error.TairRpcError;
import com.tencent.tde.client.error.TairTimeout;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.DataCache.CacheEntry;

public class ConfigCache {
	private ConcurrentHashMap<String,String> cache;
	private int cleanWaitTime; 
	private int startCapacity;
	private int nsDataTableId;
	private ClientAttr client;

	public ConfigCache(ClientAttr client,int nsDataTableId,int cleanWaitTime, int startCapacity) {
		this.client = client;
		this.cleanWaitTime = cleanWaitTime;
		this.startCapacity = startCapacity;	
		this.nsDataTableId = nsDataTableId;
		init();
	}

	public void setStartCapacity(int startCapacity) {
		this.startCapacity = startCapacity;
	}

	public int getStartCapacity() {
		return startCapacity;
	}

	public void setCleanWaitTime(int cleanWaitTime) {
		this.cleanWaitTime = cleanWaitTime;
	}

	public void init() {
		cache = new ConcurrentHashMap<String, String>(this.getStartCapacity());

		Runnable r = new Runnable() {
			public void run() {
				while (!Thread.interrupted()) {

					try {
						Thread.sleep(cleanWaitTime * 1000);
					} catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
					}
				}
			}
		};
			
		Thread t = new Thread(r);
		t.start();
		t.setPriority(Thread.MIN_PRIORITY);
	}

	public int getCacheEntryCount() {
		return cache.size();
	}

	public boolean hasKey(String key) {
		return cache.containsKey(key);
	}

	public String get(String key) {
		String ce = cache.get(key);
		if (ce == null) {
			return null;
		}
		return ce;
	}
	
	public String remove(Object key) {
		return cache.remove(key);
	}

	public String getNewValueFromTable(String key){
		TairOption opt = new TairOption(client.getTimeout());
		Result<byte[]> result;
		try {
			result = client.getClient().get((short)nsDataTableId, key.getBytes(), opt);
			if(result.isSuccess() && result.getResult()!=null){
				return new String(result.getResult());
			}
		} catch (Exception e){
			
		}
		
		return null;
	}

	public Set<String> keySet() {
		return cache.keySet();
	}
}
