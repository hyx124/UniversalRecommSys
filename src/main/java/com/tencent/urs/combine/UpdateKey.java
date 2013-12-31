package com.tencent.urs.combine;

public class UpdateKey{
	private Long uin;
	private Integer groupId;
	private String adpos;
	private String itemId;
	
	public UpdateKey(Long uin,Integer groupId,String adpos,String itemId){
		
	}
	
	public Integer getGroupId(){
		return this.groupId;
	}
	
	public Long getUin(){
		return this.uin;
	}
	
	public String getAdpos(){
		return this.adpos;
	}
	
	public String getItemId(){
		return this.itemId;
	}
}