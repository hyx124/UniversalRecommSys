package com.tencent.urs.combine;

public class UpdateKey{
	private String uin;
	private String groupId;
	private String adpos;
	private String itemId;
	
	public UpdateKey(String uin,String groupId,String adpos,String itemId){
		
	}
	
	public String getGroupId(){
		return this.groupId;
	}
	
	public String getUin(){
		return this.uin;
	}
	
	public String getAdpos(){
		return this.adpos;
	}
	
	public String getItemId(){
		return this.itemId;
	}
}