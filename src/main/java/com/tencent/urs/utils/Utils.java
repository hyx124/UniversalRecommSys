package com.tencent.urs.utils;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

public class Utils {
	
	public static enum actionType{
		Unknown(0),
		Impress(1),
		Click(2),
		PageView(3),
		Read(4),
		Save(5),
		BuyCart(6),
		Deal(7),
		Score(8),
		Comments(9),
		Reply(10),
		Ups(11),
		Praise(12),
		Share(13);
		
		private int value;  
		actionType(int value){
			this.value = value;
		}	
		
		public int getValue(){
			return this.value;
		}
	}
	
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
		if(itemId.matches("[0-9]+") && !itemId.equals("0")){
			return true;
		}
		return false;
	}
	
	public static boolean isQNumValid(Long uin){
		if(uin <10000 || uin > 10000000000L){
			return false;
		}
		return true;
	}
	
	public static boolean isGroupIdVaild(Integer groupId){
		if(groupIdSet.contains(groupId)){
			return true;
		}else{
			return false;
		}
	}

	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub	
	}

}
