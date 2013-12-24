package com.tencent.urs.asyncupdate;

import com.tencent.tde.client.TairClient.TairOption;
import com.tencent.tde.client.impl.MutiThreadCallbackClient;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;

public class UpdateCallBackContext {
	private String groupname;
	private MutiThreadCallbackClient client;
	private String updateKey;
	private byte[] updateValue;
	private TairOption putopt;
	
	
	public String getGroupname() {
		return groupname;
	}

	public MutiThreadCallbackClient getClient() {
		return client;
	}

	public String getUpdateKey() {
		return updateKey;
	}

	public byte[] getUpdateValue() {
		return updateValue;
	}

	public TairOption getPutopt() {
		return putopt;
	}


	public UpdateCallBackContext(ClientAttr clientAttr,String key,byte[] value,
									TairOption putopt) 
	{
		this.client = clientAttr.getClient();
		this.groupname = clientAttr.getGroupname();
		this.updateKey = key;
		this.updateValue = value;
		this.putopt = putopt;
	}
}
