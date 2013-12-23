package com.tencent.urs.utils;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<K, V> extends LinkedHashMap<K, V> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -9034820718190574341L;
	private LinkedHashMap<K, V> cache = null;
	private int cacheSize = 0;

	public LRUCache(int cacheSize) {
		this.cacheSize = cacheSize;
		int hashTableCapacity = (int) Math.ceil(cacheSize / 0.75f) + 1;
		cache = new LinkedHashMap<K, V>(hashTableCapacity, 0.75f, true) {
			// (an anonymous inner class)
			private static final long serialVersionUID = 1;

			@Override
			protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
				//System.out.println("size=" + size());
				return size() > LRUCache.this.cacheSize;
			}
		};
	}

	public boolean containsKey(Object key){
		return cache.containsKey(key);
	}
	
	@Override
	public int size(){
		return cache.size();
	}
	
	public V put(K key, V value) {
		return cache.put(key, value);
	}

	public V get(Object key) {
		return cache.get(key);
	}

	public static void main(String[] args) {
		LRUCache<String, String> lruCache = new LRUCache<String, String>(5);
		lruCache.put("1", "1");
		lruCache.put("2", "2");
		lruCache.put("3", "3");
		lruCache.put("4", "4");

		//System.out.println(lruCache.get("2"));
		//lruCache.get("2");
		lruCache.put("6", "6");
		lruCache.put("5", "5");
		lruCache.put("7", "7");
		lruCache.put("8", "8");
		System.out.println(lruCache.size());

	}
}