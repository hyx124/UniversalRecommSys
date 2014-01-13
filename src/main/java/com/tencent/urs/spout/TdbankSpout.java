package com.tencent.urs.spout;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;

import com.tencent.monitor.MonitorTools;
import com.tencent.urs.conf.AlgModuleConf;
import com.tencent.urs.conf.AlgModuleConf.AlgModuleInfo;
import com.tencent.urs.conf.DataFilterConf;
import com.tencent.urs.tdbank.msg.TDMsg;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.Utils;

/**
 * @author stevenxiang
 * 
 */

public class TDBankSpout implements IRichSpout {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static Logger logger = LoggerFactory
			.getLogger(TDBankSpout.class);
	public final int DEFAULT_MAX_PENDING = 100000;
	private int MAX_PENDING;
	public static byte SPEARATOR = (byte) 0xe0;
	private transient MessageConsumer messageConsumer;

	private transient MessageSessionFactory sessionFactory;
	protected SpoutOutputCollector collector;
	private transient BlockingQueue<Message> messageQueue;
	private MonitorTools mt;
	private DataFilterConf dfConf;

	public TDBankSpout(DataFilterConf dataFilterConf) {
		this.dfConf = dataFilterConf;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.spout.ISpout#open(java.util.Map,
	 * backtype.storm.task.TopologyContext,
	 * backtype.storm.spout.SpoutOutputCollector)
	 */
	public void open(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context, SpoutOutputCollector collector) {
		try {
			this.collector = collector;
			MAX_PENDING = Utils.getInt(conf, "tdbank.max.pending",DEFAULT_MAX_PENDING);
			messageQueue = new LinkedBlockingQueue<Message>();
			//this.mt = MonitorTools.getMonitorInstance(conf);

			setUpMeta(conf);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public void nextTuple() {
		boolean debug = false;
		try {
			if(debug){
				Long ActionTime = System.currentTimeMillis();
				//String[] dealMsg ={"1","UserAction",uin,uid,adpos,action_type,action_time,itemId,"","","",""}; 
				String[] msg_array = {"1","UserAction","389687043","389687043","adpos","2",String.valueOf(ActionTime/1000L),"22222","","","",""};
				dealMsgByConfig("1","UserAction",msg_array);

				Thread.sleep(1000);				
			}else{	
				Message msg = messageQueue.poll();
				if (msg != null) {
					processMessage(msg);
				}
			}
		} catch (Exception e) {
			logger.error("wrong," + e.getMessage(), e);
		}
	}

	private void processMessage(Message message)
			throws InvalidProtocolBufferException {		
		TDMsg tdmsg = TDMsg.parseFrom(message.getData());
		for (String attr : tdmsg.getAttrs()) {
			Iterator<byte[]> it = tdmsg.getIterator(attr);
			while (it.hasNext()) {
				byte[] rawMessage = it.next();
				int length = rawMessage.length;
				if (length <= 0) {
					logger.info("Msg message length is <0:");
					return ;
				} else {
					String msg = "";
					int bodyIndex = searchIndex(rawMessage, SPEARATOR);
					if (bodyIndex == -1 || bodyIndex == length - 1) {
						logger.error("check data failed,not found attr,message ="
								+ msg);
					} else {
						byte[] eventByte = Arrays.copyOfRange(rawMessage, 0,
								bodyIndex);
						msg = new String(eventByte);
						byte[] attrByte = Arrays.copyOfRange(rawMessage,
								bodyIndex + 1, rawMessage.length - 1);

						String cate = new String(attrByte);
						String[] categoryId = cate.split(",|:");
						String tag = "";
						if (categoryId.length >= 4) {
							tag = categoryId[1];
						}
						
						String[] msg_array = msg.split("\t",-1);

						if (tag.equals("yxpv")
								&& msg_array.length >= 30) {
							
							String uin = msg_array[2];
							String uid = msg_array[3];
							String action_time = msg_array[5];
							String itemId = msg_array[7];
							String adpos = msg_array[29];
							String action_type = "";
							
							if (msg_array[22].indexOf("searchex.yixun.com") >= 0
									&& msg_array[22].indexOf("searchex.yixun.com") < 15) {
								action_type = "1";
							} else if (msg_array[22].indexOf("direct enter") != -1) {
								action_type = "2";
							} else if (msg_array[22].indexOf("sale.yixun.com/morningmarket.html") != -1
									|| msg_array[22].indexOf("sale.yixun.com/nightmarket.html") != -1
									|| msg_array[22].indexOf("tuan.yixun.com") != -1) {
								action_type = "3";	
							} else if (msg_array[22].indexOf("www.baidu.com") != -1) {
								action_type = "4";
							} else if (msg_array[22].indexOf("event.yixun.com") != -1) {
								action_type = "5";
							} else{
								return;			
							}
							//output=[1, UserAction, 20806747, 160821738, 4001, 1, 1389582548, 700919, , , , ]
							String[] dealMsg ={"1","UserAction",uin,uid,adpos,action_type,action_time,itemId,"","","",""}; 
							dealMsgByConfig("1","UserAction",dealMsg);
						}else{
							return ;
						}

					}
				}
			}
		}
	}
	
	private void dealMsgByConfig(String bid,String topic,String[] msg_array){	
		HashMap<String,String> inputs = dfConf.getInputsFromArray(bid,topic,msg_array);	
		doEmit(topic,inputs);
	}

	private void doEmit(String topic,HashMap<String,String> inputs){
		if(inputs == null){
			this.collector.emit("filter_data",new Values());
			return;	
		}
	
		Values outputValues = new Values();
		for(String name: dfConf.getInputFeildsByTopic(topic)){
			if(inputs.containsKey(name)){
				outputValues.add(inputs.get(name));
			}else{
				this.collector.emit("filter_data",new Values());
				return;
			}
		}
				
		this.collector.emit(topic,outputValues);	
	}
	
	private void setUpMeta(@SuppressWarnings("rawtypes") Map conf)
			throws Exception {
		// read config
		String zk_address = Utils.get(conf, "tdbank.zookeeper.address",
				"localhost:2181");
		int zk_sessiontimeout = Utils.getInt(conf,
				"tdbank.zookeeper.sessiontimeout", 10000);
		int zk_connecttimeout = Utils.getInt(conf,
				"tdbank.zookeeper.connecttimeout", 30000);
		int zk_synctime = Utils.getInt(conf, "tdbank.zookeeper.connecttimeout",
				5000);
		int maxsize = Utils.getInt(conf, "tdbank.queue.maxsize", 50000);
		String zk_root = Utils.get(conf, "tdbank.zookeeper.metaroot", "/meta");
		String topicName = Utils.get(conf, "tdbank.topic", "ecc_51buy_real");
		String group_name = Utils.get(conf, "tdbank.group", "test-new-090314");
		// begin setup meta
		MetaClientConfig metaClientConfig = new MetaClientConfig();
		metaClientConfig.setZkConfig(new ZKConfig(zk_root, zk_address,
				zk_sessiontimeout, zk_connecttimeout, zk_synctime, true));
		sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
		ConsumerConfig consumer = new ConsumerConfig(group_name);
		consumer.setConsumeFromMaxOffset();
		messageConsumer = sessionFactory.createConsumer(consumer);
		long t = (long) (Math.random() * 60000);
		Thread.sleep(t);
		messageConsumer.subscribe(topicName, maxsize, new MessageListener() {
			public void recieveMessages(Message message) {	
				if (messageQueue.size() < MAX_PENDING) {
					messageQueue.offer(message);
				} else {
					try {
						Thread.sleep(1000L);
					} catch (InterruptedException e) {
						logger.error(e.getMessage(), e);
					}
				}
			}

			public Executor getExecutor() {
				return null;
			}
		}).completeSubscribe();
	}

	private int searchIndex(byte[] bytes, byte key) {
		int length = bytes.length;
		for (int i = length - 1; i >= 0; i--) {
			if (bytes[i] == key) {
				return i;
			}
		}
		return -1;
	}

	public void close() {
		try {
			messageConsumer.shutdown();
		} catch (final MetaClientException e) {
			logger.error("Shutdown consumer failed", e);
		}
		try {
			sessionFactory.shutdown();
		} catch (final MetaClientException e) {
			logger.error("Shutdown session factory failed", e);
		}
	}

	public void deactivate() {
		// TODO Auto-generated method stub
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public void activate() {
		// TODO Auto-generated method stub
	}

	public void ack(Object msgId) {
		// TODO Auto-generated method stub
	}

	public void fail(Object msgId) {
		// TODO Auto-generated method stub
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		for(String topic:this.dfConf.getAllTopics()){
			Fields fields = new Fields(dfConf.getInputFeildsByTopic(topic));
			declarer.declareStream(topic, fields);
		}
		declarer.declareStream("filter_data", new Fields(""));		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
	}
}
