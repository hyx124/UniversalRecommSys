package com.tencent.urs.test;

import java.text.SimpleDateFormat;

import com.tencent.urs.combine.GroupActionCombinerValue;
import com.tencent.urs.combine.UpdateKey;
import com.tencent.urs.protobuf.Recommend.ActiveType;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType;

public class ItemCountTest{
		
		private UpdateKey key;
		private GroupActionCombinerValue value;

		public ItemCountTest(){
			Long actionTime = System.currentTimeMillis()/1000L;

			this.key = new UpdateKey("1",389687043L,51,"1","23");
			this.value = new GroupActionCombinerValue(ActiveType.BuyCart,actionTime);
		}

		private UserActiveDetail genDetailData(){
			String getKey = key.getDetailKey();
			UserActiveDetail.Builder detailBuilder = UserActiveDetail.newBuilder();
			Long time = System.currentTimeMillis();
			Long today = Long.valueOf( new SimpleDateFormat("yyyyMMdd").format(time));
			for(int day =0; day < 20; day++){
				Long date = today - day;
				UserActiveDetail.TimeSegment.Builder tsBuilder = UserActiveDetail.TimeSegment.newBuilder();
				tsBuilder.setTimeId(date);
				for(int ids =1; ids <=30; ids++){
					UserActiveDetail.TimeSegment.ItemInfo.Builder itemBuilder = UserActiveDetail.TimeSegment.ItemInfo.newBuilder();
					itemBuilder.setItem(String.valueOf(ids));
					for(int act =1; act <=9; act++){
						UserActiveDetail.TimeSegment.ItemInfo.ActType.Builder actBuilder = 
								UserActiveDetail.TimeSegment.ItemInfo.ActType.newBuilder();
						actBuilder.setActType(ActiveType.valueOf(2)).setCount(1).setLastUpdateTime(time-(day*24*3600));	
						itemBuilder.addActs(actBuilder.build());
					}
					tsBuilder.addItems(itemBuilder.build());
				}
				detailBuilder.addTsegs(tsBuilder.build());
			}			
			return detailBuilder.build();
		}
		
		private Long getWinIdByTime(Long time){	
			String expireId = new SimpleDateFormat("yyyyMMdd").format(time*1000L);
			return Long.valueOf(expireId);
		}
		
		private Integer getWeight(UserActiveDetail oldValueHeap){						
			Integer newWeight = 0;					
			for(TimeSegment ts:oldValueHeap.getTsegsList()){
				if(ts.getTimeId() < getWinIdByTime(this.value.getTime() - 7*24*3600)){
					//System.out.println(ts.getTimeId());
					continue;
				}
			
				for(ItemInfo item:ts.getItemsList()){						
					if(item.getItem().equals(key.getItemId())){	
						for(ActType act: item.getActsList()){	
							if(act.getLastUpdateTime() > (this.value.getTime() -  7*24*3600)){
								newWeight =  Math.max(newWeight,act.getActType().getNumber());
								System.out.println(newWeight);
							}
						}	
					}					
				}
			}
			return newWeight;
		}

	
	
		public static void main(String[] args){
			ItemCountTest test = new ItemCountTest();
			UserActiveDetail ud = test.genDetailData();
			System.out.println(test.getWeight(ud));
		}
	}