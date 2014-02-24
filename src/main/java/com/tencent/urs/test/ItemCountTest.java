package com.tencent.urs.test;

import java.lang.ref.SoftReference;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import com.google.common.collect.ImmutableList;
import com.tencent.monitor.MonitorTools;
import com.tencent.streaming.commons.spouts.tdbank.Output;
import com.tencent.tde.client.Result;
import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.impl.MutiThreadCallbackClient;
import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBackContext;
import com.tencent.urs.combine.GroupActionCombinerValue;
import com.tencent.urs.combine.UpdateKey;
import com.tencent.urs.protobuf.Recommend;
import com.tencent.urs.protobuf.Recommend.ActiveType;
import com.tencent.urs.protobuf.Recommend.GroupCountInfo;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail;
import com.tencent.urs.protobuf.Recommend.UserCountInfo;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo;
import com.tencent.urs.protobuf.Recommend.UserActiveDetail.TimeSegment.ItemInfo.ActType;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.Utils;

public class ItemCountTest{
	private static final long serialVersionUID = 2972911860800045348L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private DataCache<GroupCountInfo> groupCountCache;
	private DataCache<UserCountInfo> userCountCache;

	private ConcurrentHashMap<UpdateKey, GroupActionCombinerValue> combinerMap;
	private ConcurrentHashMap<Recommend.ActiveType, Float> actWeightMap;
	private Long lastUpdateTime; 
	private UpdateCallBack putCallBack;	
	
	private int nsUserCountTableId;
	private int nsGroupCountTableId;
	private int nsDetailTableId;
	private int nsActWeightTableId;
	
	private int dataExpireTime;
	private int cacheExpireTime;
	private boolean debug;
	
	public class MidInfo {
		private Long timeId;
		private Float weight;
		
		MidInfo(Long timeId,Float weight){
			this.timeId = timeId;
			this.weight = weight;
		}
		
		public Long getTimeId(){
			return this.timeId;
		}
		
		public Float getWeight(){
			return this.weight;
		}
	}
	private void weightInit(){
		actWeightMap.put(Recommend.ActiveType.Impress, 0.5F);
		actWeightMap.put(Recommend.ActiveType.Click, 1F);
		actWeightMap.put(Recommend.ActiveType.PageView, 1F);
		actWeightMap.put(Recommend.ActiveType.Read, 1.5F);
		actWeightMap.put(Recommend.ActiveType.Save, 2F);
		actWeightMap.put(Recommend.ActiveType.BuyCart, 2F);
		actWeightMap.put(Recommend.ActiveType.Deal, 2F);
		actWeightMap.put(Recommend.ActiveType.Score, 3F);
		actWeightMap.put(Recommend.ActiveType.Comments, 3F);
		actWeightMap.put(Recommend.ActiveType.Reply, 3F);
		actWeightMap.put(Recommend.ActiveType.Ups, 3F);
		actWeightMap.put(Recommend.ActiveType.Praise, 4F);
		actWeightMap.put(Recommend.ActiveType.Share, 4F);
	}
	
	private Float getWeightByType(String bid,Recommend.ActiveType actionType){
		
		/*Long now = System.currentTimeMillis()/1000L;
		if(lastUpdateTime == 0){
			weightInit();
			lastUpdateTime = now;
		}else if(lastUpdateTime < (now - 3600*24)  ){
			actWeightMap.clear();
			lastUpdateTime = now;
		}*/
		
		//String actTypeKey = this.
		weightInit();		
		if(actWeightMap.containsKey(actionType)){
			return actWeightMap.get(actionType);
		}else{
			try{
				MutiThreadCallbackClient clientEntry = mtClientList.get(0).getClient();
				TairOption opt = new TairOption(mtClientList.get(0).getTimeout());
				Result<byte[]> res =  clientEntry.get((short)nsActWeightTableId, actionType.toString().getBytes(), opt);
				if(res.isSuccess() && res.getResult()!=null)
				{
						
					Recommend.ActionWeightInfo pbWeightInfo = Recommend.ActionWeightInfo.parseFrom(res.getResult());
					if(pbWeightInfo.getWeight()>=0){
						actWeightMap.put(actionType, pbWeightInfo.getWeight());
						return pbWeightInfo.getWeight();
					}
				}					
			}catch(Exception e){
				System.out.print(e.toString());
			}
		}
		return 0F;
	}

	private class GroupCountUpdateCallback implements MutiClientCallBack{
		private final MidInfo weightInfo;
		private final String groupCountKey;

		public GroupCountUpdateCallback(UpdateKey key, MidInfo weightInfo) {
			this.weightInfo = weightInfo;		
			this.groupCountKey = key.getGroupCountKey();
		}
		
		public void excute() {
			try {			
				GroupCountInfo oldWeightInfo = null;
				SoftReference<GroupCountInfo> sr = groupCountCache.get(groupCountKey);
				if(sr != null){
					oldWeightInfo = sr.get();
				}
				
				if(oldWeightInfo != null){		
					next(oldWeightInfo);
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsGroupCountTableId,groupCountKey.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}			
				
			} catch (Exception e){
				System.out.print(e.toString());
			}
		}

		private void save(String key,GroupCountInfo value){		
			synchronized(groupCountCache){
				groupCountCache.set(key, new SoftReference<GroupCountInfo>(value), cacheExpireTime);
			}
			
			System.out.print("step3 save pair count,key="+key+",value="+value.getTsegsCount());
			Future<Result<Void>> future = null;
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
				try {
					future = clientEntry.getClient().putAsync((short)nsGroupCountTableId, key.getBytes(),value.toByteArray(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,key,value.toByteArray(),putopt));
					
					/*
					if(mt!=null){
						MonitorEntry mEntryPut = new MonitorEntry(Constants.SUCCESSCODE,Constants.SUCCESSCODE);
						mEntryPut.addExtField("TDW_IDC", clientEntry.getGroupname());
						mEntryPut.addExtField("tbl_name", "FIFO1");
						mt.addCountEntry(Constants.systemID, Constants.tde_put_interfaceID, mEntryPut, 1);
					}*/
				} catch (Exception e){
					System.out.print(e.toString());
				}
			}
		}
		
		@Override
		public void handle(Future<?> future, Object context) {
			// TODO Auto-generated method stub
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			Recommend.GroupCountInfo oldWeightInfo = null;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() != null){
					oldWeightInfo = Recommend.GroupCountInfo.parseFrom(res.getResult());
				}
			} catch (Exception e) {
				System.out.print(e.toString());
			}
			next(oldWeightInfo);
		}
		
		public void next(GroupCountInfo oldWeightInfo){
			//logger.info("get step2 key="+userCountKey+",old count="+oldCount+",new count="+count);
			
			GroupCountInfo.Builder newGroupInfoBuiler = GroupCountInfo.newBuilder();
			boolean isAdded = false;
			if(oldWeightInfo != null){
				for(GroupCountInfo.TimeSegment ts:oldWeightInfo.getTsegsList()){
					if(ts.getTimeId() < weightInfo.getTimeId() - 7){
						continue;
					}
					
					Float newCount = 0F;
					if(ts.getTimeId() == weightInfo.getTimeId()){
						newCount = ts.getCount() + weightInfo.getWeight();	
						
						GroupCountInfo.TimeSegment.Builder tsBuilder = GroupCountInfo.TimeSegment.newBuilder();
						tsBuilder.setTimeId(ts.getTimeId()).setCount(newCount);
						newGroupInfoBuiler.addTsegs(tsBuilder.build());
						isAdded = true;
					}else {
						newGroupInfoBuiler.addTsegs(ts);
					}
				}
			}
			
			if(!isAdded){
				GroupCountInfo.TimeSegment.Builder tsBuilder = GroupCountInfo.TimeSegment.newBuilder();
				tsBuilder.setTimeId(weightInfo.getTimeId()).setCount(weightInfo.getWeight());
				newGroupInfoBuiler.addTsegs(tsBuilder.build());
			}

			save(groupCountKey,newGroupInfoBuiler.build());
		
		}
				
	}
	
	private class UserCountUpdateCallBack implements MutiClientCallBack{
		
		private UpdateKey key;
		private MidInfo weightInfo;
		private String userCountKey;

		public UserCountUpdateCallBack(UpdateKey key, MidInfo weightInfo) {
			this.key = key ; 
			this.weightInfo = weightInfo;	
			this.userCountKey = key.getUserCountKey();
		}
		
		public void excute() {
			try {
				UserCountInfo oldWeightInfo = null;
				SoftReference<UserCountInfo> sr = userCountCache.get(userCountKey);
				if(sr != null){
					oldWeightInfo = sr.get();
				}
				
				if(oldWeightInfo != null){		
					next(oldWeightInfo);
				}else{
					ClientAttr clientEntry = mtClientList.get(0);		
					TairOption opt = new TairOption(clientEntry.getTimeout());
					Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsUserCountTableId,userCountKey.getBytes(),opt);
					clientEntry.getClient().notifyFuture(future, this,clientEntry);	
				}			
				
			} catch (Exception e) {
				System.out.print(e.toString());
			}
		}
		
		public void next(UserCountInfo oldWeightInfo){
			//logger.info("get step2 key="+userCountKey+",old count="+oldCount+",new count="+count);
			
			UserCountInfo.Builder newUserInfoBuiler = UserCountInfo.newBuilder();
			boolean isAdded = false;
			if(oldWeightInfo != null){
				for(UserCountInfo.TimeSegment ts:oldWeightInfo.getTsegsList()){
					if(ts.getTimeId() < weightInfo.getTimeId() - 7){
						continue;
					}
					
					Float newCount = 0F;
					if(ts.getTimeId() == weightInfo.getTimeId()){
						Float changeWeight = weightInfo.getWeight()-ts.getCount();
						MidInfo newMidInfo = new MidInfo(ts.getTimeId(), changeWeight);
						new GroupCountUpdateCallback(key,newMidInfo).excute();
						
						UpdateKey noGroupKey = new UpdateKey(key.getBid(),key.getUin(),0,key.getAdpos(),key.getItemId());
						new GroupCountUpdateCallback(noGroupKey,newMidInfo).excute();
						
						newCount = ts.getCount();
						
						isAdded = true;
					}else if(ts.getCount() >= 0){
						Float changeWeight = 0 - ts.getCount();
						MidInfo newMidInfo = new MidInfo(ts.getTimeId(), changeWeight);
						
						new GroupCountUpdateCallback(key,newMidInfo).excute();
							
						UpdateKey noGroupKey = new UpdateKey(key.getBid(),key.getUin(),0,key.getAdpos(),key.getItemId());
						new GroupCountUpdateCallback(noGroupKey,newMidInfo).excute();
					}
					
					UserCountInfo.TimeSegment.Builder tsBuilder = UserCountInfo.TimeSegment.newBuilder();
					tsBuilder.setTimeId(ts.getTimeId()).setCount(newCount);
					newUserInfoBuiler.addTsegs(tsBuilder.build());
				}
			}
			
			if(!isAdded){
				UserCountInfo.TimeSegment.Builder tsBuilder = UserCountInfo.TimeSegment.newBuilder();
				tsBuilder.setTimeId(weightInfo.getTimeId()).setCount(weightInfo.getWeight());
				newUserInfoBuiler.addTsegs(tsBuilder.build());
			}

			save(userCountKey,newUserInfoBuiler.build());		
		}
		
		private void save(String userCountKey,UserCountInfo newWeightInfo){
			Future<Result<Void>> future = null;
			synchronized(userCountCache){
				userCountCache.set(userCountKey, new SoftReference<UserCountInfo>(newWeightInfo), cacheExpireTime);
			}
			
			System.out.print("step2,save usercount,key="+userCountKey+",value=");
			for(ClientAttr clientEntry:mtClientList ){
				TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
				try {
					future = clientEntry.getClient().putAsync((short)nsUserCountTableId, userCountKey.getBytes(),newWeightInfo.toByteArray(), putopt);
					clientEntry.getClient().notifyFuture(future, putCallBack, 
							new UpdateCallBackContext(clientEntry,userCountKey,newWeightInfo.toByteArray(),putopt));
					
					/*
					if(mt!=null){
						MonitorEntry mEntryPut = new MonitorEntry(Constants.SUCCESSCODE,Constants.SUCCESSCODE);
						mEntryPut.addExtField("TDW_IDC", clientEntry.getGroupname());
						mEntryPut.addExtField("tbl_name", "FIFO1");
						mt.addCountEntry(Constants.systemID, Constants.tde_put_interfaceID, mEntryPut, 1);
					}*/
				} catch (Exception e){
					System.out.print(e.toString());
				}
			}
		}
			
		@Override
		public void handle(Future<?> future, Object arg1) {
			// TODO Auto-generated method stub
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			UserCountInfo oldWeightInfo = null;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() != null){
					oldWeightInfo = UserCountInfo.parseFrom(res.getResult());
				}				
			} catch (Exception e) {
				System.out.print(e.toString());
			}
			next(oldWeightInfo);	
		}
		
	}
	
	public class ActionDetailCallBack implements MutiClientCallBack{
		private final UpdateKey key;
		private final String userCheckKey;
		private final GroupActionCombinerValue values;

		public ActionDetailCallBack(UpdateKey key, GroupActionCombinerValue values) {
			this.key = key ; 
			this.values = values;		
			this.userCheckKey = key.getDetailKey();
		}

		public void next(MidInfo weightInfo){
			if(weightInfo.getWeight() > 0){
				new UserCountUpdateCallBack(key,weightInfo).excute();
			}
		}
		
		public void excute() {
			try {
				ClientAttr clientEntry = mtClientList.get(0);		
				TairOption opt = new TairOption(clientEntry.getTimeout());
				Future<Result<byte[]>> future = clientEntry.getClient().getAsync((short)nsDetailTableId,userCheckKey.getBytes(),opt);
				clientEntry.getClient().notifyFuture(future, this,clientEntry);	
			} catch (Exception e) {
				System.out.print(e.toString());
			}
		}
			
		private MidInfo getMaxWeight(Recommend.UserActiveDetail oldValueHeap){						
			Float newWeight = getWeightByType(key.getBid(),this.values.getType());		
			Long timeId = Utils.getDateByTime(values.getTime());
			for(TimeSegment ts:oldValueHeap.getTsegsList()){
				if(ts.getTimeId() <  Utils.getDateByTime(values.getTime() - dataExpireTime)){
					continue;
				}
			
				for(ItemInfo item:ts.getItemsList()){						
					if(item.getItem().equals(key.getItemId())){	
						for(ActType act: item.getActsList()){	
							if(act.getLastUpdateTime() > (values.getTime() - dataExpireTime)){
								Float actWeight = getWeightByType(key.getBid(),act.getActType());
								if(actWeight > newWeight){
									newWeight =  Math.max(newWeight,actWeight);
									timeId = ts.getTimeId();
									System.out.print("timeId="+ts.getTimeId()+",type="+act.getActType()+",weight="+newWeight);
								}
							}
						}	
					}					
				}
			}
			System.out.print("item="+key.getItemId()+",final weight="+newWeight);
			return new MidInfo(timeId,newWeight);
		}
		
		@Override
		public void handle(Future<?> future, Object context) {			
			@SuppressWarnings("unchecked")
			Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
			try {
				Result<byte[]> res = afuture.get();
				if(res.isSuccess() && res.getResult() !=null){
					Recommend.UserActiveDetail oldValueHeap = Recommend.UserActiveDetail.parseFrom(res.getResult());
					next(getMaxWeight(oldValueHeap));
				}
			} catch (Exception e) {
				System.out.print(e.toString());
			}	
		}
	}
	
	public static void main(String[] args){
		GroupActionCombinerValue value = new GroupActionCombinerValue(Recommend.ActiveType.PageView,1393229555L);
		UpdateKey key = new UpdateKey("2",389687043L,51,"1","123");
				
		//ActionDetailCallBack cb = new ActionDetailCallBack(key,value);
	}
}