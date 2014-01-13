package com.tencent.urs.topology;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.tencent.urs.conf.AlgModuleConf;
import com.tencent.urs.conf.AlgModuleConf.AlgModuleInfo;
import com.tencent.urs.conf.DataFilterConf;
import com.tencent.urs.process.AlgDealBolt;
import com.tencent.urs.process.PretreatmentBolt;
import com.tencent.urs.process.ResultStorageBolt;
import com.tencent.urs.spout.TDBankSpout;
import com.tencent.urs.utils.Constants;

public class URSTopology {

	/**
	 * @param args
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public static void main(String[] args) throws FileNotFoundException,
			IOException {

		Config conf = new Config();
		AlgModuleConf algConf = new AlgModuleConf();
		DataFilterConf dfConf = new DataFilterConf();
		dfConf.load(new FileInputStream(args[1]));
		algConf.load(new FileInputStream(args[2]));
		
		Properties property = new Properties();
		property.load(new FileInputStream(args[0]));
		
		
	
		
		for (String key : property.stringPropertyNames()) {
			conf.put(key, property.getProperty(key));
		}

		conf.setNumWorkers(getInt(conf.get("topology.works").toString()));
		conf.setNumAckers(0);
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 100000);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("TDBankSpout", new TDBankSpout(dfConf),
				getInt(conf.get("topology.tdbank_spout.parallel").toString()));
		
		
		BoltDeclarer preBolt = builder.setBolt("PretreatBolt", new PretreatmentBolt(dfConf),
				getInt(conf.get("topology.pre_treatment.parallel").toString()));
		
		for(String topic:dfConf.getAllTopics()){
			preBolt.fieldsGrouping("TDBankSpout",topic, new Fields(dfConf.getHashKeyByTopic(topic)));
		}
					
		
		/*
		for(String algName: algConf.getAlgConfMap().keySet()){
			AlgModuleInfo algInfo = algConf.getAlgInfoByName(algName);
			builder.setBolt(algInfo.getAlgName(), new AlgDealBolt(algInfo),
					getInt(conf.get("topology."+algInfo.getAlgName()+".parallel").toString()))
					.fieldsGrouping("PretreatBolt",algInfo.getInputTopic(), new Fields("HashKey"));		
		}
		
		BoltDeclarer bolt = builder.setBolt("ResStorBolt", new ResultStorageBolt(algConf),
				getInt(conf.get("topology.result_storage.parallel").toString()));
		
		for(String algName: algConf.getAlgConfMap().keySet()){
			AlgModuleInfo algInfo = algConf.getAlgInfoByName(algName);
			if(algInfo.isStorePartition()){
				bolt.fieldsGrouping(algInfo.getAlgName(),algInfo.getAlgName(), new Fields("HashKey"));
			}
		}
		*/
		try {
			StormSubmitter.submitTopology(conf.get("topology.name").toString(),
					conf, builder.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		}

		// Topology run

	}

	private static int getInt(String value) {
		return Integer.valueOf(value);
	}
}
