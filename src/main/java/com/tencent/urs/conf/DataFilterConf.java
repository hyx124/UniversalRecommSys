package com.tencent.urs.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.jdom.*;
import org.jdom.input.SAXBuilder;

public class DataFilterConf implements Serializable{
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
	    Element element = document.getRootElement();
	        System.out.println("Root: " + element.getName());

	    // 获取子元素
	    Element hello = element.getChild("hello");
	    System.out.println("child: " + hello.getName());

	    // 获取属性
	    List list = hello.getAttributes();

	    for (int i = 0; i < list.size(); ++i)
	    {
	    	Attribute attr = (Attribute) list.get(i);
	        String attrName = attr.getName();
	        String attrValue = attr.getValue();

	        System.out.println("hello的属性： " + attrName + " = " + attrValue);
	    }

	}
}