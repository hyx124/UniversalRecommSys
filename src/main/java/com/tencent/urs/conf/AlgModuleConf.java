package com.tencent.urs.conf;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.jdom.*;
import org.jdom.input.SAXBuilder;

public class AlgModuleConf implements Serializable{	
	private String algName;
	private int expireTime;
	private String inputTopic;
	private Integer topNum;
	private Long TimeSeg;
	private String outputTableId;
	private Integer outputWinds;
		
	private HashMap<String,String> inputTables;
	private Boolean storePartition;
	private Integer algId;

	public AlgModuleConf(){
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

	public void load(XMLConfiguration algModleXML) throws ConfigurationException{	
		setAlgName(algModleXML.getString("[@name]",""));
		setAlgId(algModleXML.getInt("[@id]",0));
		setOutputTableId(algModleXML.getString("storage_table","0"));
		setExpireTime(algModleXML.getInt("expire_time",30*24*3600));
		setTopNum(algModleXML.getInt("top_num",100));
		
		setOutputTimeSeg(algModleXML.getLong("time_segs",24*3600L));
		setOutPutWinds(algModleXML.getInt("wind_nums",0));
		if(this.getOutputTableId() == 0){
			setStorePartition(true);
		}
		
		String tableName = "";
		String tableId = "";

		int idx = 0;
		while(true){
		
			tableName = algModleXML.getString("depent_tables.table("+idx+")[@name]");
			tableId = algModleXML.getString("depent_tables.table("+idx+")[@id]");
			if(tableName == null || tableName.equals("") 
					|| tableId == null || tableId.equals("") ){
				break;
			}else{
		    	inputTables.put(tableName, tableId);
		    	System.out.println("tableName="+tableName+",tablId"+tableId);
			}
			idx ++;
	    }
	   
	}
	
	public static void main(String[] args){
		AlgModuleConf algInfo = new AlgModuleConf();
		try {
			XMLConfiguration algModleXML = new XMLConfiguration("./src/main/resources/algNode.xml");
			algInfo.load(algModleXML);
		} catch (ConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
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