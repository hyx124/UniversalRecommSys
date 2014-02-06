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
		int length = message.length;
		if (length <= 0) {
			logger.info("Msg message length is <0:");
			return ;
		} 

		int bodyIndex = searchIndex(message, SPEARATOR);
		if (bodyIndex == -1 || bodyIndex == length - 1) {
			logger.error("check data failed,not found attr,message ="+ message.toString());
			return;
		}

		byte[] eventByte = Arrays.copyOfRange(message, 0, bodyIndex);
		byte[] attrByte = Arrays.copyOfRange(message, bodyIndex + 1, length - 1);

		String cate = new String(attrByte);
		String[] attrs = cate.split(",|:",-1);
		
		String categoryId = "";
		if (attrs.length >= 4) {
			categoryId = attrs[1];
		}
						
		String event = new String(eventByte);
		String[] event_array = event.split("\t",-1);
		count ++;
		
		if(true && (count %1000 == 0)){
			Long now = System.currentTimeMillis()/1000L;
			String itemId = String.valueOf(now%30+1);
			String actType = String.valueOf(now%10);
			String qq = "389687043";
			String category = String.valueOf(now%10);
			String fa_category = String.valueOf(now%10+100);
			
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
			return;
		}
		
		
		if (categoryId.equals("item_detail_info") && event_array.length >= 17) {	
			String impDate = event_array[0];
			String bid = event_array[1];
			String itemId = event_array[2];
			String categoryId1 = event_array[3];
			String categoryId2 = event_array[4];
			String categoryId3 = event_array[5];
			String categoryName1 = event_array[6];
			String categoryName2 = event_array[7];
			String categoryName3 = event_array[8];
			String free = event_array[9];
			String publish = event_array[10];
			String price = event_array[11];
			
			String text = event_array[12];
			String itemTime = event_array[13];
			String expireTime = event_array[14];
			String platForm = event_array[15];
			String score = event_array[16];
					
			//hash_key,topic,bid,imp_date,item_id,cate_id1,cate_id2,cate_id3,
			//cate_name1,cate_name2,cate_name3,free,publish,price,text,item_time,
			//expire_time,plat_form,score
			String[] dealMsg ={itemId,categoryId,bid,impDate,itemId,categoryId1,categoryId2,categoryId3,
						categoryName1,categoryName2,categoryName3,free,publish,price,text,itemTime,expireTime,platForm,score}; 
			dealMsgByConfig("1",categoryId,dealMsg);
		}else if (categoryId.equals("user_detail_info") && event_array.length >= 8) {	
			String impDate = event_array[0];
			String bid = event_array[1];
			String qq = event_array[2];
			String imei = event_array[3];
			String uid = event_array[4];
			String level = event_array[5];
			String regDate = event_array[6];
			String regTime = event_array[7];
					
			//hash_key,topic,bid,imp_date,qq,imei,uid,level,reg_date, reg_time
			String[] dealMsg ={qq,categoryId,bid,impDate,qq,imei,uid,level,regDate,regTime};
			dealMsgByConfig("1",categoryId,dealMsg);
		}else if (categoryId.equals("action_weight_info") && event_array.length >= 4) {	
			String impDate = event_array[0];
			String bid = event_array[1];
			String actType = event_array[2];
			String weight = event_array[3];
			
			//hash_key,topic,bid,imp_date,type_id,weight
			String[] dealMsg ={actType,categoryId,bid,impDate,actType,weight};
			dealMsgByConfig("1",categoryId,dealMsg);
		}else if(categoryId.equals("category_level_info") && event_array.length >= 6){
			String impDate = event_array[0];
			String bid = event_array[1];
			String cateId = event_array[2];
			String cateName = event_array[3];
			String level = event_array[4];
			String fatherId = event_array[5];
			
			//hash_key,topic,bid,imp_date,cate_id,cate_name,level,father_id
			String[] dealMsg ={cateId,categoryId,bid,impDate,cateId,cateName,level,fatherId};
			dealMsgByConfig("1",categoryId,dealMsg);
		}
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
