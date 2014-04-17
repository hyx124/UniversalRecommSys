package com.tencent.urs.utils;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class Utils {
	
	private static HashSet<Integer> groupIdSet = new HashSet<Integer>();
	static{
		groupIdSet.add(11);
		groupIdSet.add(12);
		groupIdSet.add(21);
		groupIdSet.add(22);
		groupIdSet.add(31);
		groupIdSet.add(32);
		groupIdSet.add(41);
		groupIdSet.add(42);
		groupIdSet.add(51);
		groupIdSet.add(52);
		groupIdSet.add(61);
		groupIdSet.add(62);
		groupIdSet.add(71);
		groupIdSet.add(72);
		groupIdSet.add(81);
		groupIdSet.add(82);
		groupIdSet.add(91);
		groupIdSet.add(92);
		groupIdSet.add(10);
	}
	
	private static HashMap<Integer,Float> actWeightMap = new HashMap<Integer,Float>();
	static{
		actWeightMap.put(1, 0F);
		actWeightMap.put(2, 1F);
		actWeightMap.put(3, 1F);
		actWeightMap.put(4, 1.5F);
		actWeightMap.put(5, 2F);
		actWeightMap.put(6, 2F);
		actWeightMap.put(7, 2F);
		actWeightMap.put(8, 2F);
		actWeightMap.put(9, 2F);
		actWeightMap.put(10, 2F);
		actWeightMap.put(11, 2F);
		actWeightMap.put(12, 2.5F);
		actWeightMap.put(13, 2.5F);
		
		//spcial for news
		actWeightMap.put(16, 1.5F);
		actWeightMap.put(17, 1.5F);
		actWeightMap.put(18, 1.5F);
		actWeightMap.put(25, 2F);
		actWeightMap.put(26, 2F);	
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
	
	public static boolean isItemIdValid(String itemId){
		if(itemId.equals("")){
			return false;
		}
		return true;
	}
		
	public static boolean isPageIdValid(String pageId){
		if(pageId.equals("")){
			return false;
		}
		return true;
	}
	
	public static boolean isQNumValid(String qq){	
		if(!qq.matches("[0-9]+") || qq.equals("0") || qq.length() <= 4){
			return false;
		}
		return true;
	}
	
	public static boolean isActionTimeValid(String time){	
		if(!time.matches("[0-9]+") || time.equals("0") || time.length() < 9){
			return false;
		}
		return true;
	}
	
	public static boolean isGroupIdVaild(String groupId){
		if(!groupId.matches("[0-9]+") || groupId.equals("0")){
			return false;
		}else{
			if(groupIdSet.contains(Integer.valueOf(groupId))){
				return true;
			}else{
				return false;
			}
		}
	}

	public static Float getActionWeight(Integer actionType) {
		if(actWeightMap.containsKey(actionType)){
			return actWeightMap.get(actionType);
		}else{
			return null;
		}
	}

	public static boolean isRecommendAction(String actionType){
		if(actionType.equals("1") || actionType.equals("2")){
			return true;
		}
		return false;
	}
	
	public static boolean isBidValid(String bid) {
		return true;
	}
	
	public static String getAlgKey(String bid,String itemId,String adpos,String algName,String groupId){
		StringBuffer getKey = new StringBuffer(bid);		
		getKey.append("#").append(itemId)
			.append("#").append(adpos)
			.append("#").append(algName)
			.append("#").append(groupId);	
		return getKey.toString();
	}
	
	public static String getDetailKey(String bid,String adpos,String uin,String algName){
		StringBuffer getKey = new StringBuffer(bid);		
		getKey.append("#").append(adpos)
			.append("#").append(uin)
			.append("#").append(algName);
		return getKey.toString();
	}
	
	public static Long getDateByTime(Long time){	
		String expireId = new SimpleDateFormat("yyyyMMdd").format(time*1000L);
		return Long.valueOf(expireId);
	}
	
	public static String spliceStringBySymbol(String symbol, String... args){
		StringBuffer spliceRes = new StringBuffer();	
		
		for(int idx = 0;idx < args.length - 1; idx++){
			spliceRes.append(args[idx]).append("#");
		}
		
		return spliceRes.append(args[args.length-1]).toString();
	}
	
	public static String getItemPairKey(String itemId1,String itemId2){
		StringBuffer getKey = new StringBuffer();		
		if(itemId1.compareTo(itemId2) < 0){
			getKey.append(itemId1).append("#").append(itemId2);
			
		}else{
			getKey.append(itemId2).append("#").append(itemId1);
		}
		
		return getKey.toString();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println(getDateByTime(1393743909L - 7*3600*24L));
		String test = spliceStringBySymbol("#","a","b","c");
		System.out.println(test);
	}


}
