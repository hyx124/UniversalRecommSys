package com.tencent.urs.bolts;

import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.tencent.streaming.commons.bolts.config.AbstractConfigUpdateBolt;
import com.tencent.streaming.commons.spouts.tdbank.Output;
import com.tencent.urs.conf.AlgModuleConf;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;

import backtype.storm.tuple.Tuple;


public class CtrStorBolt extends AbstractConfigUpdateBolt{

	private List<ClientAttr> mtClientList;	
	private ClientAttr mainClient;
	private AlgModuleConf algInfo;
	private static Logger logger = LoggerFactory.getLogger(CtrStorBolt.class);
	
	public CtrStorBolt(String config, ImmutableList<Output> outputField, String sid) {
		super(config, outputField, sid);
		this.updateConfig(super.config);
	}

	@Override
	public void updateConfig(XMLConfiguration config) {
		try {
			this.algInfo.load(config);
		} catch (ConfigurationException e) {
			logger.error(e.toString());
		}
	}

	@Override
	public void processEvent(String sid, Tuple tuple) {
		
	}

}