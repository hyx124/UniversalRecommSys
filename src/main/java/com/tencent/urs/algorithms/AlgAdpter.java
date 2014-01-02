package com.tencent.urs.algorithms;

import com.tencent.urs.conf.AlgModuleConf.AlgModuleInfo;

import backtype.storm.tuple.Tuple;

public interface AlgAdpter{
	//public void input(ArrayList<String> inputs);
	public void deal(AlgModuleInfo algInfo, Tuple input);

}