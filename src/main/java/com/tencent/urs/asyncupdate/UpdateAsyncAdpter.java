package com.tencent.urs.asyncupdate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Future;

import com.taobao.tair.client.Result;
import com.taobao.tair.client.TairClient.TairOption;
import com.taobao.tair.client.error.TairFlowLimit;
import com.taobao.tair.client.error.TairQueueOverflow;
import com.taobao.tair.client.error.TairRpcError;
import com.taobao.tair.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.monitor.MonitorEntry;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;

	
public class UpdateAsyncAdpter  {
		private final String key;
		private final byte[] values;
		//private final List<ClientAttr> mtClientList;
		
		public UpdateAsyncAdpter(String key,byte[] values) {
			this.key = key ;
			this.values = values;
		}

		public void excute() {
			
		}

		public void update(){
		

		}
		
		public byte[] mergeValue(){
			return null;
			
		}
	}


	