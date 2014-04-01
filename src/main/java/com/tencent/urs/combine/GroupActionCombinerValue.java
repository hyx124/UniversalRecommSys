package com.tencent.urs.combine;

import java.io.Serializable;
import com.tencent.urs.protobuf.Recommend;


public class GroupActionCombinerValue implements Combiner<GroupActionCombinerValue>,Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 305886042186738812L;
	private Long time;
	private Integer actType;
	
	public Long getTime(){
		return this.time;
	}
	
	public Integer getType(){
		return this.actType;
	}
	
	public GroupActionCombinerValue(Integer actType,Long time){
		this.time = time;
		this.actType = actType;
	}
		
	@Override
	public void incrument(GroupActionCombinerValue newValue) {		
		if(newValue.getType() > this.getType()){
			this.actType = newValue.getType();
			this.time = newValue.getTime();
		}
	}
	
	public static void main(String[] args){
		//System.out.print(Recommend.ActiveType.Impress.getNumber());
	}
	
}