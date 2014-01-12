package com.tencent.urs.conf;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.jdom.*;
import org.jdom.input.SAXBuilder;

import com.tencent.urs.conf.AlgModuleConf.AlgModuleInfo;

public class DataFilterConf implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private class TopicInfo{
		private String name;
		private String[] fields;
		private Boolean isNeedGroupId;
		private Boolean isNeedQQ;
		
		public TopicInfo(){
			name = "";
			fields = new String[0];
			isNeedGroupId = false;
			isNeedQQ = false;
		}
		
		public Integer getFieldCount(){
			return fields.length;
		}
		
		public String[] getFields(){
			return fields;
		}
		
		public Boolean isNeedQQ(){
			return isNeedQQ;
		}
		
		public Boolean isNeedGroupId(){
			return isNeedGroupId;
		}

		public void setName(String topicName) {
			this.name = topicName;
		}

		public void setFeilds(String inFields) {
			this.fields = inFields.split(",",-1);
		}

		public void setIsNeedQQ(String needQQ) {
			if(needQQ.equalsIgnoreCase("true")){
				this.isNeedQQ = true;
			}else{
				this.isNeedQQ = false;
			}
			
		}

		public void setIsNeedGroupId(String needGroupId) {
			if(needGroupId.equalsIgnoreCase("true")){
				this.isNeedGroupId = true;
			}else{
				this.isNeedGroupId = false;
			}
		}
	}
	
	private class ColumnInfo{
		private String colName;
		private Boolean isInRange;
		private Boolean isNotNull;
		private Long maxValue;
		private Long minValue;
		
		public ColumnInfo(){
			colName = "";
			isInRange = false;
			isNotNull = false;
			maxValue = 0L;
			minValue = 0L;
		}
		
		public void setName(String colName) {

			this.colName = colName;
		}
		
		public void setValidRange(String valueRange) {
			String[] values = valueRange.replace("[","").replace("]","")
								.replace("(","").replace(")","").split(",");
			
			
			if(values.length == 2){
				if(values[0].matches("[0-9]+") && values[1].matches("[0-9]+")){
					
					minValue = Long.valueOf(values[0]);
					maxValue = Long.valueOf(values[1]);
					if(colName.equals("qq")){
						System.out.println(minValue+","+maxValue);
					}
					this.isInRange = true;
				}	
			}	
		}
		
		public void setNotNull(String notNull) {
			if(notNull.equalsIgnoreCase("true")){
				this.isNotNull = true;
			}else{
				this.isNotNull = false;
			}
			
		}
		
		public Boolean isInRange(){
			return this.isInRange;
		}
		
	}
	
	private HashMap<String ,TopicInfo> allInfoMap;
	private HashMap<String ,HashMap<String,HashMap<String,ColumnInfo>>> dfConfMap;
	
	
	public DataFilterConf(){
		this.allInfoMap = new HashMap<String ,TopicInfo>();
		this.dfConfMap = new HashMap<String ,HashMap<String,HashMap<String,ColumnInfo>>>();
	}
	
	private boolean checkValue(ColumnInfo cinfo, String value) {
		if(value == null || value.equalsIgnoreCase("null")){
			if(cinfo.isNotNull){
				return false;
			}
		}
		
		if(cinfo.isInRange()){
			if(value==null || !value.matches("[0-9]+") ){
				return false;
			}
			
			if(Long.valueOf(value) > cinfo.maxValue || Long.valueOf(value) < cinfo.minValue){
				return false;
			}
		
		}
		
		return true;
	}
	
	public HashMap<String,String> getInputsFromArray(String bid, String topic, String[] msgArray){
		if(!allInfoMap.containsKey(topic) ||
				!dfConfMap.containsKey(bid) || 
				!dfConfMap.get(bid).containsKey(topic)){
			return null;
		}

		TopicInfo tInfo = allInfoMap.get(topic);
		if(msgArray.length != tInfo.getFieldCount()){
			return null;
		}	
		
		
		HashMap<String,String> resMap = new HashMap<String,String>();
		
		String[] fields = tInfo.getFields();
		for(Integer idx=0 ;idx <  msgArray.length ;idx++){
			String colName = fields[idx];
			String colValue = msgArray[idx];
					
			if(dfConfMap.get(bid).get(topic).containsKey(colName)){
				ColumnInfo cinfo = dfConfMap.get(bid).get(topic).get(colName);
				if(checkValue(cinfo,colValue)){
					resMap.put(colName, colValue);
				}else{
					return null;
				}
			}else{
				resMap.put(colName, colValue);
			}	
		}

		return resMap;
	}
	
	public String getDefaultValue(String topic,String column){
		return "";
	}
	
	public void load(FileInputStream fileInputStream){
		// 构造
		SAXBuilder saxBuilder = new SAXBuilder();

	    // 获取文档
	    Document document = null;
		try {
			document = saxBuilder.build(fileInputStream);
		} catch (JDOMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		   // 得到根元素
	    Element root = document.getRootElement();
	    Element allNode = root.getChild("AllInfo");
	    List<Element> topicNods = allNode.getChildren("Topic");
	    for(Element topic: topicNods){
	    	TopicInfo topicInfo = new TopicInfo();
	    	String topicName =  topic.getChildTextTrim("Name");
	    	List<Element> topicAttrList = topic.getChildren();
	    	for(Element attr: topicAttrList){
		    	if(attr.getName().equals("Name")){
		    		topicInfo.setName(topicName);
	    		}else if(attr.getName().equals("Fields")){
	    			topicInfo.setFeilds(topic.getChildTextTrim("Fields"));
	    		}else if(attr.getName().equals("IsNeedQQ")){
	    			topicInfo.setIsNeedQQ(topic.getChildTextTrim("IsNeedQQ"));
	    		}else if(attr.getName().equals("IsNeedGroup")){
	    			topicInfo.setIsNeedGroupId(topic.getChildTextTrim("IsNeedGroup"));
	    		}
		    }
	    	allInfoMap.put(topicName, topicInfo);
	    }
	    
	    Element buNode = root.getChild("BusinessInfo");
	    String bid = buNode.getChildTextTrim("Bid");
	    List<Element> buNods = buNode.getChildren("Topic");
	    
	    HashMap<String,HashMap<String, ColumnInfo>> topicInfoMap = new HashMap<String,HashMap<String, ColumnInfo>>();
	    for(Element topic: buNods){
	    	String topicName =  topic.getChildTextTrim("Name");
	    	
	    	List<Element> columns = topic.getChildren("Column");
	    	
	    	HashMap<String, ColumnInfo> cInfoMap = new HashMap<String, ColumnInfo>();
	    	for(Element col: columns){
	    		List<Element> attrsList = col.getChildren();
	    		String colName = col.getChildTextTrim("Name");
	    		
	    		ColumnInfo cInfo = new ColumnInfo();
	    		for(Element attr: attrsList){
			    	if(attr.getName().equals("Name")){		
			    		cInfo.setName(colName);
		    		}else if(attr.getName().equals("ValidRange")){
		    			cInfo.setValidRange(col.getChildTextTrim("ValidRange"));
		    		}else if(attr.getName().equals("NotNull")){
		    			cInfo.setNotNull(col.getChildTextTrim("NotNull"));
		    		}
			    }
	    		cInfoMap.put(colName, cInfo);	
		    }
	    	topicInfoMap.put(topicName, cInfoMap);
	    	
	    }
	    dfConfMap.put(bid, topicInfoMap);
	}

	public String[] getInputFeildsByTopic(String topic) {
		if(allInfoMap.containsKey(topic)){
			return allInfoMap.get(topic).getFields();
		}
		return null;
	}

	public Set<String> getAllTopics() {
		return allInfoMap.keySet();
	}

	public boolean isNeedQQ(String topic) {
		if(allInfoMap.containsKey(topic)){
			return allInfoMap.get(topic).isNeedQQ();
		}
		return false;
	}

	public boolean isNeedGroupId(String topic) {
		if(allInfoMap.containsKey(topic)){
			return allInfoMap.get(topic).isNeedGroupId();
		}
		return false;
	}
	
	public static void main(String[] args){
		DataFilterConf conf = new DataFilterConf();
		try {
			conf.load(new FileInputStream("./src/main/resources/filter.xml"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		for(String topic: conf.allInfoMap.keySet()){
			System.out.println(topic);
			TopicInfo tinfo = conf.allInfoMap.get(topic);
			System.out.println("fieldsCount="+tinfo.getFieldCount());
			for(String field:tinfo.getFields()){
				System.out.print(field+",");	
			}
			System.out.println("");
			System.out.println(""+tinfo.isNeedGroupId());
			System.out.println(""+tinfo.isNeedQQ());
		}
		
		System.out.println("---------------------------------");
		for(String bid:conf.dfConfMap.keySet()){
			System.out.println("bid="+bid);
			for(String topic:conf.dfConfMap.get(bid).keySet()){
				System.out.println("topic="+topic);
				for(String col:conf.dfConfMap.get(bid).get(topic).keySet()){
					ColumnInfo cinfo = conf.dfConfMap.get(bid).get(topic).get(col);
					System.out.println("---------------------------------");
					System.out.println("colName="+cinfo.colName);
					System.out.println("colMaxValue="+cinfo.maxValue);
					System.out.println("colMinValue="+cinfo.minValue);
					System.out.println("isInRange="+cinfo.isInRange);
					System.out.println("isNotNull="+cinfo.isNotNull);
				}
			}
		
		}
		
		//bid,topic,qq,uid,adpos,action_type,action_time,item_id,action_result,imei,platform,lbs_info
		String[] msg_array = "1,UserAction,389687043,17400,adpos,2,1391187661,12345,0,imei,platform,lbs_info".split(",");
		HashMap<String,String> inputs = conf.getInputsFromArray("1","UserAction",msg_array);	
		if(inputs != null){
			for(String key: inputs.keySet()){
				System.out.println("key="+key+"-----value="+inputs.get(key));
			}
		}
		
		

	}
}