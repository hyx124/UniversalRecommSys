package com.tencent.urs.combine;

public class UpdateKey{
	private String bid;
	private Long uin;
	private Integer groupId;
	private String adpos;
	private String itemId;
	
	public UpdateKey(String bid,Long uin,Integer groupId,String adpos,String itemId){
		this.bid = bid;
		this.uin = uin;
		this.groupId = groupId;
		this.adpos = adpos;
		this.itemId = itemId;
	}
	
	public String getBid(){
		return this.bid;
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