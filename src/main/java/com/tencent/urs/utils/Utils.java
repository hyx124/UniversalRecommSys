package com.tencent.urs.utils;

import java.util.HashSet;
import java.util.Map;

import com.tencent.urs.protobuf.Recommend.ActiveType;

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
	
	public static boolean isQNumValid(String qq){
		if(!qq.matches("[0-9]+") && qq.equals("0")){
			return false;
		}else{
			if(Long.valueOf(qq) <10000 || Long.valueOf(qq) > 10000000000L){
				return false;
			}
		}
		
		return true;
	}
	
	public static boolean isGroupIdVaild(String groupId){
		if(!groupId.matches("[0-9]+") && groupId.equals("0")){
			return false;
		}else{
			if(groupIdSet.contains(Long.valueOf(groupId))){
				return true;
			}else{
				return false;
			}
		}

	}

	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub	
	}

	public static ActiveType getActionTypeByString(String actionType) {
		if(actionType.equals("1")){
			return ActiveType.Impress;
		}else if(actionType.equals("2")){
			return ActiveType.Click;
		}else if(actionType.equals("3")){
			return ActiveType.PageView;
		}else if(actionType.equals("4")){
			return ActiveType.Read;
		}else if(actionType.equals("5")){
			return ActiveType.Save;
		}else if(actionType.equals("6")){
			return ActiveType.BuyCart;
		}else if(actionType.equals("7")){
			return ActiveType.Deal;
		}else if(actionType.equals("8")){
			return ActiveType.Score;
		}else if(actionType.equals("9")){
			return ActiveType.Comments;
		}else if(actionType.equals("10")){
			return ActiveType.Reply;
		}else if(actionType.equals("11")){
			return ActiveType.Ups;
		}else if(actionType.equals("12")){
			return ActiveType.Praise;
		}else if(actionType.equals("13")){
			return ActiveType.Share;
		}else{
			return ActiveType.Unknown;
		}
	}

	public static boolean isBidValid(String bid) {
		// TODO Auto-generated method stub
		return true;
	}

}
