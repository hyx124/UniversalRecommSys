package com.tencent.urs.utils;

import java.lang.ref.SoftReference;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;

import com.tencent.urs.protobuf.Recommend.UserPairInfo;

public class DataCache<T> {
	private final AtomicInteger persistCount = new AtomicInteger(0);
	private boolean selfClean = true;
	private int cleanWaitTime = 20; // 20 seconds by default
	private ConcurrentHashMap<String, CacheEntry<T>> cache;
	private static Logger logger = Logger.getLogger(DataCache.class);

	private int keyValidTime = 60;

	private int startCapacity = 50000;

	public DataCache(Map conf) {
		this.cleanWaitTime = Utils.getInt(conf, "cache.wait.time", 20);
		this.startCapacity = Utils.getInt(conf, "cache.start.capacity",50000);
		this.selfClean = Utils.getBoolean(conf, "cache.self.clean", true);
		this.keyValidTime = Utils.getInt(conf, "cache.alive.time", 60);
		init();
	}

	public void setStartCapacity(int startCapacity) {
		this.startCapacity = startCapacity;
	}

	public int getStartCapacity() {
		return startCapacity;
	}

	public void setKeyValidTime(int keyValidTime) {
		this.keyValidTime = keyValidTime;
	}

	public void setSelfClean(boolean selfClean) {
		this.selfClean = selfClean;
	}

	public void setCleanWaitTime(int cleanWaitTime) {
		this.cleanWaitTime = cleanWaitTime;
	}

	public void init() {
		cache = new ConcurrentHashMap<String, CacheEntry<T>>(
				this.getStartCapacity());

		if (selfClean) {
			Runnable r = new Runnable() {
				public void run() {
					while (!Thread.interrupted()) {
						int cleanCount = cleanOutGarbage();
						logger.debug("Cleaned out " + cleanCount
								+ " entries; HoldCache has " + cache.size()
								+ " entries");
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
	}

	public int getQueueSize() {
		return 0;
	}

	public int getPersistCount() {
		return persistCount.get();
	}

	public int getCacheEntryCount() {
		return cache.size();
	}

	public boolean hasKey(String key) {
		return cache.containsKey(key);
	}

	public void set(String key, SoftReference<T> value, int period) {
		persistCount.getAndIncrement();
		CacheEntry<T> ce = new CacheEntry<T>();
		ce.value = value;
		ce.period = period;
		ce.addTime = System.currentTimeMillis();
		cache.put(key, ce);
	}

	public void set(String key, SoftReference<T> value) {
		set(key, value, keyValidTime);
	}

	public SoftReference<T> get(String key) {
		CacheEntry<T> ce = cache.get(key);
		if (ce == null) {
			return null;
		}

		return ce.value;
	}

	public Map<Object, Object> getBulk(String[] keys) {
		HashMap<Object, Object> map = new HashMap<Object, Object>();
		for (String key : keys) {
			Object value = get(key);
			if (value != null) {
				map.put(key, value);
			}
		}
		return map;
	}

	public Object getObject(String key) {
		return get(key);
	}

	public Map<Object, Object> getBulkObjects(String[] keys) {
		return getBulk(keys);
	}

	public Object remove(Object key) {
		return cache.remove(key).value;
	}

	public void clear() {
		cache.clear();
	}

	public int cleanOutGarbage() {
		int count = 0;
		for (Enumeration en = cache.keys(); en.hasMoreElements();) {
			Object key = en.nextElement();
			CacheEntry ce = cache.get(key);
			if (ce != null && ce.isExpired()) {
				count++;
				cache.remove(key);
			}
		}
		return count;
	}

	public Set<String> keySet() {
		return cache.keySet();
	}

	public class CacheEntry<T> {
		SoftReference<T> value;
		long addTime;
		int period;

		public boolean isExpired() {
			if (period > 0) {
				if ((addTime + (1000 * (long) period)) <= System
						.currentTimeMillis()) {
					return true;
				}
			}
			return false;
		}
	}

}
