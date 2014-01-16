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
	private Long time;
	private Recommend.ActiveType actType;
	
	public Long getTime(){
		return this.time;
	}
	
	public Recommend.ActiveType getType(){
		return this.actType;
	}
	
	public GroupActionCombinerValue(Recommend.ActiveType actType,Long time){
		this.time = time;
		this.actType = actType;
	}
		
	@Override
	public void incrument(GroupActionCombinerValue newValue) {		
		if(newValue.getType().getNumber() > this.getType().getNumber()){
			this.actType = newValue.getType();
			this.time = newValue.getTime();
		}
	}
	
	public static void main(String[] args){
		System.out.print(Recommend.ActiveType.Impress.getNumber());
	}
	
}