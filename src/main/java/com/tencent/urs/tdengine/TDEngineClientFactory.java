package com.tencent.urs.tdengine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.tair.client.impl.MutiThreadCallbackClient;

public class TDEngineClientFactory {
	private static Logger logger = LoggerFactory
			.getLogger(TDEngineClientFactory.class);

	private TDEngineClientFactory() {

	}

	public static List<ClientAttr>createMTClientList(
			Map<String, String> configMap) {
		
		List<ClientAttr> clientEntryList = new ArrayList<ClientAttr>();
			
		String[] configServerList = configMap.get("list.tdengine.configserver").split(";");
		String[] timeOutList = configMap.get("list.tdengine.timeout").split(";");
		String[] groupNameList = configMap.get("list.tdengine.groupname").split(";");
		if (configServerList.length > 0
				&& configServerList.length == timeOutList.length
				&& configServerList.length == groupNameList.length) {

			for (int i = 0; i < configServerList.length; i++) {

				try {
					MutiThreadCallbackClient client = new MutiThreadCallbackClient();
					client.setGroup(groupNameList[i]);
					client.setWorkerThreadCount(10);
					client.setThreadNumber(10);
					String[] servers = configServerList[i].split(",");
					if (servers.length == 1) {
						client.setMaster(servers[0]);
					} else if (servers.length > 1) {
						client.setMaster(servers[0]);
						client.setSlave(servers[1]);
					}
					client.setMaxNotifyQueueSize(100000);
					client.init();
					
					ClientAttr cla = new ClientAttr(groupNameList[i], Integer.parseInt(timeOutList[i]),client);
					clientEntryList.add(cla);
					//mtClientMap.put(
					//		new ClientAttr(groupNameList[i], Integer
					//				.parseInt(timeOutList[i])), client);
				} catch (Exception e) {
					logger.error("init client:" + groupNameList[i] + "; err info:"
							+ e.toString());
				}
			}
		}
		return clientEntryList;
	}

	public static MutiThreadCallbackClient createMTClient(
			Map<String, String> configMap) {
		String configServer = configMap.get("tdengine.configserver");
		String groupName = configMap.get("tdengine.groupname");
		MutiThreadCallbackClient client = null;
		try {
			client = new MutiThreadCallbackClient();
			client.setGroup(groupName);
			client.setWorkerThreadCount(10);
			client.setThreadNumber(10);
			String[] servers = configServer.split(",");
			if (servers.length == 1) {
				client.setMaster(servers[0]);
			} else if (servers.length > 1) {
				client.setMaster(servers[0]);
				client.setSlave(servers[1]);
			}
			client.setMaxNotifyQueueSize(100000);
			client.init();
		} catch (Exception e) {
			logger.error("init client:" + groupName + "; err info:" + e.toString());
		}
		return client;
	}

	public static class ClientAttr {
		private String groupname;
		private int timeout;
		private MutiThreadCallbackClient client;

		public ClientAttr(String groupname, int timeout, MutiThreadCallbackClient client) {
			this.groupname = groupname;
			this.timeout = timeout;
			this.client = client;
		}

		public String getGroupname() {
			return this.groupname;
		}

		public int getTimeout() {
			return this.timeout;
		}
		
		public MutiThreadCallbackClient getClient(){
			return this.client;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((groupname == null) ? 0 : groupname.hashCode());
			result = prime * result + timeout;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ClientAttr other = (ClientAttr) obj;

			if (groupname == null) {
				if (other.groupname != null)
					return false;
			} else if (!groupname.equals(other.groupname))
				return false;
			if (timeout != other.timeout)
				return false;
			return true;
		}
	}

	public static void main(String[] args) {
		System.out.println("get integer:" + Integer.getInteger("5"));
	}

}
