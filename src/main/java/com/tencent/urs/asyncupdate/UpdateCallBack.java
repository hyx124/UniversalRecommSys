package com.tencent.urs.asyncupdate;

import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.monitor.MonitorEntry;
import com.tencent.monitor.MonitorTools;
import com.tencent.tde.client.Result;
import com.tencent.tde.client.Result.ResultCode;
import com.tencent.tde.client.impl.MutiThreadCallbackClient.MutiClientCallBack;
import com.tencent.urs.utils.Constants;



public class UpdateCallBack implements MutiClientCallBack {
	private static Logger logger = LoggerFactory
			.getLogger(UpdateCallBack.class);
	private boolean debug;
	private MonitorTools mt;
	private String inifidCode;
	private String inifidQuery;
	private String systemID;

	public UpdateCallBack(MonitorTools mt, String systemID,
			String interfaceID, String inifidQuery) {
		this.mt = mt;
		this.systemID = systemID;
		this.inifidCode = interfaceID;
		this.inifidQuery = inifidQuery;
	}

	@Override
	public void handle(Future<?> future, Object context) {
		Future<Result<byte[]>> afuture = (Future<Result<byte[]>>) future;
		Result<byte[]> result;
		UpdateCallBackContext updateData = (UpdateCallBackContext) context;
		
		try {
			result = afuture.get();
			if (result.getCode().equals(ResultCode.OK)) {				
				if(this.mt!=null){
					MonitorEntry mEntryPut = new MonitorEntry(Constants.SUCCESSCODE,Constants.SUCCESSCODE);
					mEntryPut.addExtField("TDW_IDC", updateData.getGroupname());
					mEntryPut.addExtField("tbl_name", inifidQuery);
					this.mt.addCountEntry(systemID, inifidCode, mEntryPut, 1);
				}
			} else {
				if(this.mt!=null){
					MonitorEntry mEntryPut = new MonitorEntry(Constants.BUSSINESS_ERRORCODE,result.getCode().errno());
					mEntryPut.addExtField("TDW_IDC", updateData.getGroupname());
					mEntryPut.addExtField("tbl_name", inifidQuery);
					this.mt.addCountEntry(systemID,inifidCode,mEntryPut,1);
				}
			}
		} catch (Exception e) {
			if (debug) {
				logger.info(e.toString());
			}
		}

	}
}
