package com.tencent.urs.algorithms;

import backtype.storm.tuple.Tuple;

public interface AlgAdpter{
	//public void input(ArrayList<String> inputs);
	public void deal(Tuple input);
}