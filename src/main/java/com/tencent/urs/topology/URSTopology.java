package com.tencent.urs.topology;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.tencent.urs.utils.Constants;
import com.tencent.urs.conf.AlgModuleConf;
import com.tencent.urs.process.AlgDealBolt;
import com.tencent.urs.process.PretreatmentBolt;
import com.tencent.urs.spout.TdbankSpout;

public class URSTopology {

	/**
	 * @param args
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public static void main(String[] args) throws FileNotFoundException,
			IOException {

		Config conf = new Config();
		Properties property = new Properties();
		property.load(new FileInputStream(args[0]));
		for (String key : property.stringPropertyNames()) {
			conf.put(key, property.getProperty(key));
		}

		conf.setNumWorkers(getInt(conf.get("topology.works").toString()));
		conf.setNumAckers(0);
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 100000);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("TDBankSpout", new TdbankSpout(),
				getInt(conf.get("topology.pv_spout.parallel").toString()));
	
		
		ArrayList<AlgModuleConf> algList = new ArrayList<AlgModuleConf>();
		
		for(AlgModuleConf alg: algList){
			builder.setBolt(alg.getAlgName(), new AlgDealBolt(alg.getAlgName()),
					getInt(conf.get("topology."+alg.getAlgName()+".parallel").toString()))
					.fieldsGrouping("InputComputer",alg.getInputStream(), new Fields(alg.getHashKey()));		
		}
		
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