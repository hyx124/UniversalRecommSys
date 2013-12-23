package com.tencent.urs.algorithms;

import java.util.List;

import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;

import backtype.storm.tuple.Tuple;


public class CollectorFilter implements AlgAdpter{
	
	private List<ClientAttr> mtClientList;	
	private ClientAttr mainClient;
	
	
	public CollectorFilter(List<ClientAttr> mtClientList){
		this.mtClientList = mtClientList;
		this.mainClient = mtClientList.get(0);
	}

	@Override
	public void deal(Tuple input) {
		// TODO Auto-generated method stub
		
		//step1,query freq(item) from group_detail
		
			
		//step2,query pairs(item,item) from user_action_detail
		
		//step3,query freq(item,item) from group_cocurrent_detail
		
		//step4,formula
	}
}