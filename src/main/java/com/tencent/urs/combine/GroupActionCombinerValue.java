package com.tencent.urs.combine;

import java.io.Serializable;
import java.util.HashMap;

import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.UserActiveHistory.ActiveRecord;
import com.tencent.urs.utils.Utils;

public class GroupActionCombinerValue implements Combiner<GroupActionCombinerValue>,Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 305886042186738812L;
	private HashMap<Utils.actionType,Integer> actRecordMap;
	public class Action{
		
	}
	
	public HashMap<Utils.actionType,Integer> getActRecodeMap(){
		return this.actRecordMap;
	}
	
	public GroupActionCombinerValue(){
		this.actRecordMap = new HashMap<Utils.actionType,Integer>();
	}
	
	public void init(Utils.actionType actType,Integer count){
		this.actRecordMap.clear();
		actRecordMap.put(actType,count);
	}
		
	@Override
	public void incrument(GroupActionCombinerValue other) {
		
		for(Utils.actionType at: other.getActRecodeMap().keySet()){
			int count = other.getActRecodeMap().get(at);
			if(this.getActRecodeMap().containsKey(at)){
				count = count + actRecordMap.get(at);
			}			
			actRecordMap.put(at, count);
		}
	}

	
}