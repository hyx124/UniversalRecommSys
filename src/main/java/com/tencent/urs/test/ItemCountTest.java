package com.tencent.urs.test;

import java.util.concurrent.ConcurrentHashMap;

import com.tencent.tde.client.Result;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.impl.MutiThreadCallbackClient;
import com.tencent.urs.bolts.ItemCountBolt.MidInfo;
import com.tencent.urs.combine.GroupActionCombinerValue;
import com.tencent.urs.combine.UpdateKey;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.CtrInfo;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType;
import com.tencent.urs.utils.Utils;

public class ItemCountTest{
	
	private static ConcurrentHashMap<Integer, Float> actWeightMap = 
			new ConcurrentHashMap<Integer, Float>();
	
	private static Float getWeightByType(String bid,Integer actionType){
		return Utils.getActionWeight(actionType);
	}
	
	private static void getMaxWeight(UpdateKey key, GroupActionCombinerValue values,Recommend.UserActiveDetail oldValueHeap){						
		Float newWeight = getWeightByType("2",values.getType());		
		Long timeId = Utils.getDateByTime(values.getTime());
		for(TimeSegment ts:oldValueHeap.getTsegsList()){
			if(ts.getTimeId() <  Utils.getDateByTime(values.getTime() - 7*3600*24)){
				continue;
			}
		
			for(ItemInfo item:ts.getItemsList()){						
				if(item.getItem().equals(key.getItemId())){	
					for(ActType act: item.getActsList()){	
							Float actWeight = getWeightByType(key.getBid(),act.getActType());
							if(actWeight > newWeight){
								newWeight =  Math.max(newWeight,actWeight);
								timeId = ts.getTimeId();
								System.out.println("timeId="+ts.getTimeId()+",type="+act.getActType()+",weight="+newWeight);
							}
						
					}	
				}					
			}
		}
		System.out.println("item="+key.getItemId()+",final weight="+newWeight);
	}
	
	public static Recommend.UserActiveDetail genActiveDetailInfo(){
		Recommend.UserActiveDetail.Builder oldValueHeap = Recommend.UserActiveDetail.newBuilder();
		
		for(int day= 20140226 ;day > 20140201; day--){
			Recommend.UserActiveDetail.TimeSegment.Builder tsBuilder = 
					Recommend.UserActiveDetail.TimeSegment.newBuilder();
			tsBuilder.setTimeId(day);
			
			for(int itemId = 1 ; itemId < 200 ; itemId++){
				Recommend.UserActiveDetail.TimeSegment.ItemInfo.Builder itemBuilder = 
						Recommend.UserActiveDetail.TimeSegment.ItemInfo.newBuilder();
				
				itemBuilder.setItem(String.valueOf(itemId));
				
				Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType.Builder actBuilder1=
						Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType.newBuilder();
				
				actBuilder1.setActType(3).setCount(1).setLastUpdateTime(0);
				
				Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType.Builder actBuilder2=
						Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType.newBuilder();
				
				actBuilder2.setActType(2).setCount(1).setLastUpdateTime(0);
				
				if(day <= 20140224){
					Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType.Builder actBuilder3=
							Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType.newBuilder();
					
					actBuilder3.setActType(7).setCount(1).setLastUpdateTime(0);
					itemBuilder.addActs(actBuilder3.build());
				}
				
				
				Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType.Builder actBuilder4=
						Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType.newBuilder();
				
				actBuilder4.setActType(1).setCount(1).setLastUpdateTime(0);
				
				itemBuilder.addActs(actBuilder1.build()).addActs(actBuilder2.build())
						.addActs(actBuilder4.build());
				
				tsBuilder.addItems(itemBuilder.build());
			}
			oldValueHeap.addTsegs(tsBuilder.build());
		}
		
		return oldValueHeap.build();
	}
	
	public static void main(String[] args){
		Long time = System.currentTimeMillis()/1000;
		
		GroupActionCombinerValue value = new GroupActionCombinerValue(3,time);
		UpdateKey key = new UpdateKey("2",389687043L,51,"1","123");
		
		
		Recommend.UserActiveDetail oldHeap = genActiveDetailInfo();
		getMaxWeight(key, value, oldHeap);
		//ActionDetailCallBack cb = new ActionDetailCallBack(key,value);
	}
}