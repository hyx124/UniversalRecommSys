package com.tencent.urs.combine;

import java.io.Serializable;
import java.util.HashMap;

import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.UserActiveHistory.ActiveRecord;
import com.tencent.urs.utils.Utils;

public class ActionCombinerValue implements Combiner<ActionCombinerValue>,Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 305886042186738812L;
	private HashMap<String,Recommend.UserActiveHistory.ActiveRecord> actRecordMap;
	public class Action{
		
	}
	
	public HashMap<String,Recommend.UserActiveHistory.ActiveRecord> getActRecodeMap(){
		return this.actRecordMap;
	}
	
	public ActionCombinerValue(){
		this.actRecordMap = new HashMap<String,Recommend.UserActiveHistory.ActiveRecord>();
	}
	
	public void init(String itemId,Recommend.UserActiveHistory.ActiveRecord actRecord){
		this.actRecordMap.clear();
		actRecordMap.put(itemId, actRecord);
	}
		
	@Override
	public void incrument(ActionCombinerValue other) {		
		for(String itemId: other.getActRecodeMap().keySet()){
			actRecordMap.put(itemId, other.getActRecodeMap().get(itemId));
		}
	}

	
}