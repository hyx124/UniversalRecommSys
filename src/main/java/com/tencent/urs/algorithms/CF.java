package com.tencent.urs.algorithms;

import java.util.List;

import com.tencent.urs.conf.AlgModuleConf.AlgModuleInfo;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;

import backtype.storm.tuple.Tuple;


public class CF implements AlgAdpter{
	
	private List<ClientAttr> mtClientList;	
	private ClientAttr mainClient;
	
	
	public CF(List<ClientAttr> mtClientList){
		this.mtClientList = mtClientList;
		this.mainClient = mtClientList.get(0);
	}

	@Override
	public void deal(AlgModuleInfo algInfo, Tuple input) {
		// TODO Auto-generated method stub
		
	}
}