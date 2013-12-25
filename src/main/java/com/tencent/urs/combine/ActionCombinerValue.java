package com.tencent.urs.combine;

import java.io.Serializable;
import java.util.HashMap;

import com.tencent.urs.utils.Utils;

public class ActionCombinerValue implements Combiner<ActionCombinerValue>,Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 305886042186738812L;

	
	private HashMap<Utils.actionType,Integer> actRecodeMap;
	
	public HashMap<Utils.actionType,Integer> getActRecodeMap(){
		return this.actRecodeMap;
	}
	
	public ActionCombinerValue(){
		this.actRecodeMap = new HashMap<Utils.actionType,Integer>();
	}
	
	public void init(Utils.actionType act){
		this.actRecodeMap.clear();
		actRecodeMap.put(act, 1);
	}
		
	@Override
	public void incrument(ActionCombinerValue other) {
		// TODO Auto-generated method stub
		for(Utils.actionType act :other.getActRecodeMap().keySet()){
			if(actRecodeMap.containsKey(act)){
				actRecodeMap.put(act, other.getActRecodeMap().get(act) + this.getActRecodeMap().get(act));
			}else{
				actRecodeMap.put(act, other.getActRecodeMap().get(act));
			}
		}
		
	}	
}