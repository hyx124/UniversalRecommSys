package com.tencent.urs.spouts;

import java.util.Arrays;
import java.util.Map;

import com.tencent.streaming.commons.spouts.tdbank.Output;
import com.tencent.streaming.commons.spouts.tdbank.TdbankSpout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import com.tencent.monitor.MonitorTools;
import com.tencent.urs.conf.DataFilterConf;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;

import backtype.storm.tuple.Values;
import com.tencent.urs.utils.Utils;

public class BaseInfoSpout extends TdbankSpout {
	
	private static final long serialVersionUID = -779488162448649143L;
	private static Logger logger = LoggerFactory
			.getLogger(BaseInfoSpout.class);
	public static byte SPEARATOR = (byte) 0xe0;

	protected SpoutOutputCollector collector;
	private MonitorTools mt;
	private DataFilterConf dfConf;
	private boolean debug;
	private int count;

	public BaseInfoSpout(String config, ImmutableList<Output> outputField) {
		super(config, outputField);
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context, SpoutOutputCollector collector) {
		super.open(conf, context, collector);
		this.collector = collector;
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.debug = Utils.getBoolean(conf, "debug", false);
	}

	@Override
	public void processMessage(byte[] message){	
		Long now = System.currentTimeMillis()/1000L;
		String itemId = String.valueOf(now%30+1);
		String actType = String.valueOf(now%10);
		String qq = "389687043";
		String category = String.valueOf(now%10);
		String fa_category = String.valueOf(now%10+100);
		if(count %1000 == 0){
			/*<output_fields>
			<stream_id>item_detail_info</stream_id>
			<fields>hash_key,topic,bid,imp_date,item_id,cate_id1,cate_id2,cate_id3,cate_name1,cate_name2,cate_name3,free,publish
			,price,text,item_time,expire_time,plat_form,score</fields>
		</output_fields>
		
		<output_fields>
			<stream_id>user_detail_info</stream_id>
			<fields>hash_key,topic,bid,imp_date,qq,imei,uid,level,reg_date, reg_time</fields>
		</output_fields>
		
		<output_fields>
			<stream_id>action_weight_info</stream_id>
			<fields>hash_key,topic,bid,imp_date,type_id,weight</fields>
		</output_fields>
		
		<output_fields>
			<stream_id>category_level_info</stream_id>
			<fields>hash_key,topic,bid,imp_date,cate_id,cate_name,level,father_id</fields>
		</output_fields>*/
			
			String[] dealMsg1 ={"1#"+itemId,"item_detail_info","1","20140126",
					itemId,"1","2","3","大类1","中类2","小类3","0","0","10.0","--",String.valueOf(now),String.valueOf(now+3000000),"iOS;Android","100"}; 
			String[] dealMsg2 ={"1#"+qq,"user_detail_info","1","20130126",
					qq,"IMEI12222","17139104","5","20140126",String.valueOf(now)}; 
			
			String[] dealMsg3 ={"1#"+actType,"action_weight_info","1","20140126",
					actType,actType}; 
			String[] dealMsg4 ={"1#"+category,"category_level_info","1","20140126",
					category,"类目"+category,category,fa_category}; 
			
			dealMsgByConfig("1","item_detail_info",dealMsg1);
			dealMsgByConfig("1","user_detail_info",dealMsg2);
			dealMsgByConfig("1","action_weight_info",dealMsg3);
			dealMsgByConfig("1","category_level_info",dealMsg4);
			count = 0;
		}else{
			this.collector.emit("filter_data",new Values(""));
		}
		count ++;
	}
	
	private void dealMsgByConfig(String bid,String topic,String[] msg_array){	
		if(msg_array == null){
			this.collector.emit("filter_data",new Values(""));
			return;	
		}
	
		Values outputValues = new Values();
		for(String value: msg_array){
			outputValues.add(value);
		}
		this.collector.emit(topic,outputValues);			
	}
	
	private int searchIndex(byte[] bytes, byte key) {
		int length = bytes.length;
		for (int i = length - 1; i >= 0; i--) {
			if (bytes[i] == key) {
				return i;
			}
		}
		return -1;
	}

}
