package com.tencent.urs.utils;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

public class Utils {
	
	public static class MiddleInfo
	{
		private String primaryKey;
		private Long compareKey;
	    private HashMap<String,String> attrMap;
	    
	    public MiddleInfo(String primaryKey,Long compareKey){
	    	this.primaryKey = primaryKey;
	    	this.compareKey = compareKey;
	    }
	    
	    public void setAttr(String key,String value){
	    	//if(attrMap.containsKey(key))
	    	attrMap.put(key, value);
	    }
	    
	    public String getPrimaryKey(){
	    	return this.primaryKey;
	    }
	    
	    public Long getCompareKey(){
	    	return this.compareKey;
	    }
	}
	
	private static HashSet<String> priceFilterSet = new HashSet<String>();
	static{
		priceFilterSet.add("1828");
		priceFilterSet.add("1847");
		priceFilterSet.add("1866");
		priceFilterSet.add("1885");
		priceFilterSet.add("1904");
		priceFilterSet.add("1923");
		priceFilterSet.add("1942");
		priceFilterSet.add("928");
		priceFilterSet.add("929");
		priceFilterSet.add("931");
		priceFilterSet.add("932");
		priceFilterSet.add("935");
		priceFilterSet.add("1826");
		priceFilterSet.add("1827");
		priceFilterSet.add("1845");
		priceFilterSet.add("1846");
		priceFilterSet.add("1864");
		priceFilterSet.add("1865");
		priceFilterSet.add("1883");
		priceFilterSet.add("1884");
		priceFilterSet.add("1902");
		priceFilterSet.add("1903");
		priceFilterSet.add("1921");
		priceFilterSet.add("1922");
		priceFilterSet.add("1940");
		priceFilterSet.add("1941");
	}
	
	public static String get(Map conf, String key, String default_value) {
		Object o = conf.get(key);
		if (o != null) {
			return o.toString();
		} else {
			return default_value;
		}
	}

	public static int getInt(Map conf, String key, int default_value) {
		Object o = conf.get(key);
		if (o != null) {
			return Integer.parseInt(o.toString());
		} else {
			return default_value;
		}
	}

	public static boolean getBoolean(Map conf, String key, boolean default_value) {
		Object o = conf.get(key);
		if (o != null) {
			if (o.toString().equalsIgnoreCase("true"))
				return true;
			else
				return false;
		} else {
			return default_value;
		}
	}
	
	public static boolean isCidValid(String cid){
		if(cid.length() < 12 && cid.matches("[0-9]+") && !cid.equals("0")){
			return true;
		}
		return false;
	}
	
	public static boolean isUinValid(String uin){
		if(uin.equals("") || !uin.matches("[0-9]+")  || Long.valueOf(uin) <10000 
				|| Long.valueOf(uin) > 10000000000L){
			return false;
		}
		return true;
	}
	
	public static boolean isTypeVaild(String type){

		if(!type.matches("[0-9]+") 
				|| type.equals("0") 
				|| type.equals("990")
				|| type.equals("393")
				|| type.equals("474")
				|| type.equals("400")
				|| type.equals("469")
				|| type.equals("470")
				|| type.equals("472")){
			return false;
		}else{
			return true;
		}
	}
	
	public static boolean isLowFloor(String poolId){	
		if(priceFilterSet.contains(poolId)){
			return true;
		}
		return false;
	}
	
	public static Long getTimeSeg(Long uinxTime, int delayMin) {
		String nTime_str = new SimpleDateFormat("yyyyMMddHHmm").format(uinxTime
				+ delayMin * 60 * 1000L);
		Long nTime_long = Long.valueOf(nTime_str);
		Long timeHour = nTime_long / 100;
		Long timeSeg = (nTime_long % 100) / 30 * 30;
		return timeHour * 100L + timeSeg;
	}

	public static void insertToTopList(LinkedList<MiddleInfo> minHeap,
			MiddleInfo newVal, int topNum) {
		boolean insert = false;
		for (int idx = 0; idx < minHeap.size(); idx++) {
			MiddleInfo eachVal = minHeap.get(idx);
			if (!insert && eachVal.compareKey <= newVal.compareKey){
				minHeap.add(idx, newVal);
				idx++;
				insert = true;
			}
			if (eachVal.primaryKey.equals(newVal.primaryKey)) {
				minHeap.remove(eachVal);
				idx--;
			}
		}
		
		if (minHeap.size() > topNum) {
			minHeap.removeLast();
		} else if (minHeap.size() < topNum && !insert) {
			minHeap.add(newVal);
		}

	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub	
		LinkedList<MiddleInfo> minHeap = new LinkedList<MiddleInfo>();
		
		MiddleInfo minfo1 = new MiddleInfo("a",123L);
		Utils.insertToTopList(minHeap,minfo1,10);
		
		MiddleInfo minfo2 = new MiddleInfo("b",124L);
		Utils.insertToTopList(minHeap,minfo2,10);
		
		MiddleInfo minfo3 = new MiddleInfo("c",125L);
		Utils.insertToTopList(minHeap,minfo3,10);
		
		MiddleInfo minfo4 = new MiddleInfo("d",126L);
		Utils.insertToTopList(minHeap,minfo4,10);
		
		MiddleInfo minfo5 = new MiddleInfo("e",127L);
		Utils.insertToTopList(minHeap,minfo5,10);
		
		MiddleInfo minfo6 = new MiddleInfo("f",128L);
		Utils.insertToTopList(minHeap,minfo6,10);
		
		MiddleInfo minfo7 = new MiddleInfo("g",129L);
		Utils.insertToTopList(minHeap,minfo7,10);
		
		MiddleInfo minfo8 = new MiddleInfo("h",130L);
		Utils.insertToTopList(minHeap,minfo8,10);
		
		MiddleInfo minfo9 = new MiddleInfo("i",131L);
		Utils.insertToTopList(minHeap,minfo9,10);
		
		MiddleInfo minfo10 = new MiddleInfo("i",32L);
		Utils.insertToTopList(minHeap,minfo10,10);
		
		MiddleInfo minfo11 = new MiddleInfo("k",133L);
		Utils.insertToTopList(minHeap,minfo11,10);
		
		MiddleInfo minfo12 = new MiddleInfo("l",134L);
		Utils.insertToTopList(minHeap,minfo12,10);
		
		MiddleInfo minfo13 = new MiddleInfo("m",135L);
		Utils.insertToTopList(minHeap,minfo13,10);
		
		for(MiddleInfo eachVal:minHeap){
			System.out.println(eachVal.getPrimaryKey()+","+eachVal.getCompareKey());
		}
	}

}
