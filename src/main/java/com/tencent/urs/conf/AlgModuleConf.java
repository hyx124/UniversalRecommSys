package com.tencent.urs.conf;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.jdom.*;
import org.jdom.input.SAXBuilder;

public class AlgModuleConf implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private HashMap<String,AlgModuleInfo> algModuleMap;
	
	public HashMap<String,AlgModuleInfo> getAlgConfMap(){
		return this.algModuleMap;
	}
	
	public AlgModuleInfo getAlgInfoByName(String algName){
		return this.algModuleMap.get(algName);
	}
	
	public AlgModuleConf(){
		this.algModuleMap = new HashMap<String,AlgModuleInfo>();
	}
	
	
	
	public class AlgModuleInfo{
		private String algName;
		private int expireTime;
		private String inputTopic;
		private Integer topNum;
		private Long TimeSeg;
		private String outputTableId;
		private Integer outputWinds;
		private Long updateTime;
		
		private HashMap<String,String> inputTables;
		private Boolean storePartition;
		private Integer algId;
		
	
		public AlgModuleInfo(){
			inputTables = new HashMap<String,String>();
		}
		
		public void setAlgName(String algName) {
			this.algName = algName;
		}

		public void setExpireTime(int expireTime) {
			this.expireTime = expireTime;
		}
		
		public void setInputTopic(String inputTopic) {
			this.inputTopic = inputTopic;
		}
		

		public void setTopNum(Integer topNum) {
			this.topNum = topNum;
		}

		public void setOutputTimeSeg(Long TimeSeg) {
			this.TimeSeg = TimeSeg;
			
		}

		public void setOutputTableId(String outputTableId) {
			this.outputTableId = outputTableId;
			
		}

		public void setOutPutWinds(Integer winds) {
			this.outputWinds = winds;
		}
		
		public void setUpdateTime(long time) {
			this.updateTime = time;
		}

		public void setStorePartition(Boolean boolean1) {
			this.storePartition = boolean1;
		}

		public Integer getAlgId(){
			return this.algId;
		}
		
		public String getAlgName(){
			return this.algName;
		}

		public Integer getTopNum() {
			return this.topNum;
		}

		public short getOutputTableId() {
			return Short.valueOf(this.outputTableId);
		}

		public int getDataExpireTime() {
			return this.expireTime;
		}

		public Long getUpdateTime() {
			return this.updateTime;
		}

		public String getInputTableIdByName(String name) {
			return this.inputTables.get(name);
		}

		public String getInputTopic() {
			return this.inputTopic;
		}

		public boolean isStorePartition() {
			return this.storePartition;
		}

		public int getCacheExpireTime() {
			return 10*60;
		}

		public void setAlgId(Integer algId) {
			this.algId = algId;
		}
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
	    List<Element> algMod = root.getChildren("AlgMod");

	    for(Element algInfo: algMod){
	    	AlgModuleInfo algModInfo = new AlgModuleInfo();
	    	List<Element> algAttrList = algInfo.getChildren();
	    	String algName = algInfo.getChildTextTrim("AlgName");
	    	
	    	for(Element attr: algAttrList){
	    		if(attr.getName().equals("AlgId")){
	    			algModInfo.setAlgId(Integer.valueOf(algInfo.getChildTextTrim("AlgId")));
	    		}else if(attr.getName().equals("AlgName")){
	    			algModInfo.setAlgName(algName);
	    		}else if(attr.getName().equals("InputTopic")){
	    			algModInfo.setInputTopic(algInfo.getChildTextTrim("InputTopic"));
	    		}else if(attr.getName().equals("OutputTableId")){
	    			algModInfo.setOutputTableId(algInfo.getChildTextTrim("OutputTableId"));
	    		}else if(attr.getName().equals("ExpireTime")){
	    			algModInfo.setExpireTime(Integer.valueOf(algInfo.getChildTextTrim("ExpireTime")));
	    		}else if(attr.getName().equals("TopNum")){
	    			algModInfo.setTopNum(Integer.valueOf(algInfo.getChildTextTrim("TopNum")));
	    		}else if(attr.getName().equals("OutPutTimeSeg")){
	    			algModInfo.setOutputTimeSeg(Long.valueOf(algInfo.getChildTextTrim("OutPutTimeSeg")));
	    		}else if(attr.getName().equals("OutPutWinds")){
	    			algModInfo.setOutPutWinds(Integer.valueOf(algInfo.getChildTextTrim("OutPutWinds")));
	    		}else if(attr.getName().equals("StorePartition")){
	    			algModInfo.setStorePartition(Boolean.valueOf(algInfo.getChildTextTrim("StorePartition")));
	    		}else if(attr.getName().equals("InputTable")){
	    			List<Element> inputTables = attr.getChildren();
	    	        for(Element table: inputTables){
	    	        	algModInfo.inputTables.put(table.getChildTextTrim("TableName"), table.getChildTextTrim("TableId"));
	    	        }
	    		}
	    	}

	        algModInfo.setUpdateTime (System.currentTimeMillis()/1000L);
	    	
	    	
	    	this.algModuleMap.put(algName, algModInfo);
	    }
	}
	
	public static void main(String[] args){
		AlgModuleConf conf = new AlgModuleConf();
		try {
			conf.load(new FileInputStream("./src/main/resources/alg.xml"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		for(String name:conf.getAlgConfMap().keySet()){
			AlgModuleInfo algInfo = conf.getAlgInfoByName(name);
			System.out.println(name);
			System.out.println("id="+algInfo.getAlgId());
			System.out.println("name="+algInfo.getAlgName());
			System.out.println("InputTopic="+algInfo.getInputTopic());
			System.out.println("OutputTableId="+algInfo.getOutputTableId());
			System.out.println("TopNum="+algInfo.getTopNum());
			System.out.println("ExpireTime="+algInfo.expireTime);
			System.out.println("yes="+algInfo.inputTables.size());
			for(String tableName:algInfo.inputTables.keySet()){
				System.out.println("name="+tableName+","+algInfo.getInputTableIdByName(tableName));
			}
			System.out.println("----------------------------------");
		}
		
		
	}

}