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
		
		Properties property = new Properties();
		property.load(new FileInputStream(args[0]));
		
		//dfConf.load(new FileInputStream(args[1]));
		//algConf.load(new FileInputStream(args[2]));
	
		
		for (String key : property.stringPropertyNames()) {
			conf.put(key, property.getProperty(key));
		}

		conf.setNumWorkers(getInt(conf.get("topology.works").toString()));
		conf.setNumAckers(0);
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 100000);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("TDBankSpout", new TDBankSpout(dfConf),
				getInt(conf.get("topology.tdbank_spout.parallel").toString()));
		
		builder.setBolt("PretreatBolt", new PretreatmentBolt(algConf),
				getInt(conf.get("topology.pre_treatment.parallel").toString()))
				.fieldsGrouping("TDBankSpout",Constants.user_info_stream, new Fields("bid","uid"))
				.fieldsGrouping("TDBankSpout",Constants.item_info_stream, new Fields("bid","item_id"))
				//.fieldsGrouping("TDBankSpout",Constants.item_category_stream, new Fields("bid","category_id"))
				//.fieldsGrouping("TDBankSpout",Constants.action_weight_stream, new Fields("item_id","type_id"))
				.fieldsGrouping("TDBankSpout",Constants.actions_stream, new Fields("bid","qq","uid"));	
			
		for(String algId: algConf.getAlgConfMap().keySet()){
			AlgModuleInfo algInfo = algConf.getAlgInfoById(algId);
			builder.setBolt(algInfo.getAlgName(), new AlgDealBolt(algInfo),
					getInt(conf.get("topology."+algInfo.getAlgName()+".parallel").toString()))
					.fieldsGrouping("PretreatBolt",algInfo.getInputStream(), new Fields(algInfo.getHashKey()));		
		}
		
		BoltDeclarer bolt = builder.setBolt("ResStorBolt", new ResultStorageBolt(algConf),
				getInt(conf.get("topology.result_storage.parallel").toString()));
		
		for(String algId: algConf.getAlgConfMap().keySet()){
			AlgModuleInfo algInfo = algConf.getAlgInfoById(algId);
			if(algInfo.isDealByCenter()){
				bolt.fieldsGrouping(algInfo.getAlgName(),Constants.user_info_stream, new Fields("key"));
			}
		
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
