package com.tencent.urs.conf;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.jdom.*;
import org.jdom.input.SAXBuilder;

public class AlgModuleConf implements Serializable{
	

	private ArrayList<AlgModuleInfo> algModuleList;
	
	public ArrayList<AlgModuleInfo> getAlgList(){
		return this.algModuleList;
	}
	
	public AlgModuleConf(){
		this.algModuleList = new ArrayList<AlgModuleInfo>();
	}
	
	
	public class AlgModuleInfo{
		private String algName;
		private String inputStream;
		private String hashKey;
		
		public String getAlgName(){
			return algName;
		}
		
		public String getInputStream(){
			return inputStream;
		}	
		
		public String getHashKey(){
			return hashKey;
		}

		public Object getTopicName() {
			return null;
		}

		public boolean isNeedGroupId() {
			// TODO Auto-generated method stub
			return false;
		}

		public String getOutputStream() {
			// TODO Auto-generated method stub
			return null;
		}

		public String getOutputFields() {
			// TODO Auto-generated method stub
			return null;
		}
	}

	public void load(FileInputStream fileInputStream){
		// ����
		SAXBuilder saxBuilder = new SAXBuilder();

	    // ��ȡ�ĵ�
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

	    // �õ���Ԫ��
	    Element element = document.getRootElement();
	        System.out.println("Root: " + element.getName());

	    // ��ȡ��Ԫ��
	    Element alg_mod = element.getChild("alg_mod");
	    System.out.println("child: " + alg_mod.getName());

	    // ��ȡ����
	    List list = alg_mod.getAttributes();
	    
	    System.out.println("child size: " + list.size());
	    for (int i = 0; i < list.size(); ++i)
	    {
	    	Attribute attr = (Attribute) list.get(i);
	        String attrName = attr.getName();
	        String attrValue = attr.getValue();

	        System.out.println("hello�����ԣ� " + attrName + " = " + attrValue);
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
	}

}