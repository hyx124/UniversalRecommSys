package com.tencent.urs.combine;
/*
 * detailKey = bid#adpos#uin#ActionDetail
 * userPairCountKey = bid#adpos#item1#item2#uin
 * groupPairCountKey = bid#adpos#item1#item2#groupId
 * */
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
	
	//bid#adpos#uin#ActionDetail
	public String getDetailKey(){
	
		StringBuffer getKey = new StringBuffer(bid);		
		getKey.append("#").append(uin)
				.append("#").append("ActionDetail");
		return getKey.toString();
	}
	
	public String getImpressDetailKey(){
		
		StringBuffer getKey = new StringBuffer(bid);		
		getKey.append("#").append(uin)
				.append("#").append("ImpressDetail");
		return getKey.toString();
	}
	
	//userCountKey = bid#item1#uin
	public String getUserCountKey(){
		StringBuffer getKey = new StringBuffer(bid);		
		getKey.append("#").append(itemId)
			.append("#").append(uin);
		return getKey.toString();
	}
	
	//userPairKey = bid#item1#item2#uin
	public String getUserPairKey(){
		StringBuffer getKey = new StringBuffer(bid);		
		getKey.append("#").append(uin);
		return getKey.toString();
	}

	//GroupCountKey = bid#item1#groupId
	public String getGroupCountKey(){
		StringBuffer getKey = new StringBuffer(bid);		
		getKey.append("#").append(itemId)
			.append("#").append(groupId);
		return getKey.toString();
	}
	
	public String getOtherGroupCountKey(String otherItemId){
		StringBuffer getKey = new StringBuffer(bid);		
		getKey.append("#").append(otherItemId)
			.append("#").append(groupId);
		return getKey.toString();
	}
	
	public String getGroupPairKey(String otherItem){
		StringBuffer getKey = new StringBuffer(bid);		
		if(itemId.compareTo(otherItem) < 0){
			getKey.append("#").append(itemId).append("#").append(otherItem);
			
		}else{
			getKey.append("#").append(otherItem).append("#").append(itemId);
		}
		
		getKey.append("#").append(groupId);
		return getKey.toString();
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
	
	public static void main(String[] args){
		UpdateKey key = new UpdateKey("1",389687043L,51,"adpos","123");
		System.out.println(key.getDetailKey());
		System.out.println(key.getUserCountKey());
		System.out.println(key.getGroupCountKey());
		System.out.println(key.getUserPairKey());
		System.out.println(key.getGroupPairKey("345"));
	}
}