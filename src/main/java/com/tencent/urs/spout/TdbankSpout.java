package com.tencent.urs.spout;

import java.util.ArrayList;
import java.util.Arrays;
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

@SuppressWarnings("serial")
public class TdbankSpout implements IRichSpout {
	private static Logger logger = LoggerFactory
			.getLogger(TdbankSpout.class);
	public final int DEFAULT_MAX_PENDING = 50000;
	private int MAX_PENDING;
	public static byte SPEARATOR = (byte) 0xe0;
	private transient MessageConsumer messageConsumer;

	private transient MessageSessionFactory sessionFactory;
	protected SpoutOutputCollector collector;
	private transient BlockingQueue<Message> messageQueue;
	private MonitorTools mt;
	private DataFilterConf dfConf;

	public TdbankSpout(DataFilterConf dataFilterConf) {
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
			MAX_PENDING = Utils.getInt(conf, "tdbank.max.pending",
					DEFAULT_MAX_PENDING);
			messageQueue = new LinkedBlockingQueue<Message>();
			this.mt = MonitorTools.getMonitorInstance(conf);

			setUpMeta(conf);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.spout.ISpout#nextTuple()
	 */
	public void nextTuple() {
		boolean debug = true;
		try {
			if(debug){
				this.collector.emit("",new Values("",""));
				
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

						String[] msg_array = msg.split("\t");
						if(!isNeedFilter(msg_array)){
							RouteData(msg_array);
						}
					}
				}
			}
		}
	}

	private boolean isNeedFilter(String[] inputTuples){
		return true;
	}
	
	private void RouteData(String[] inputTuples){	
		String algName = "ads";
		if(inputTuples[0].equals("")){
			String uin = inputTuples[1];			
		}
		
		this.collector.emit(Constants.actions_stream,new Values(algName,""));
		return;
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.spout.ISpout#close()
	 */
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.spout.ISpout#deactivate()
	 */
	public void deactivate() {
		// TODO Auto-generated method stub
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.topology.IComponent#getComponentConfiguration()
	 */
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
		
		declarer.declareStream(Constants.user_info_stream, 
				new Fields("topic","bid","imp_date","qq","imei","uid","level","reg_date","reg_time"));
		
		declarer.declareStream(Constants.item_info_stream, 
				new Fields("topic","bid","imp_date","item_id","categroy_id1","categroy_id2","categroy_id3",
						"category_name1","category_name2","category_name3",
						"free","publish","price","text","item_time","expire_time","platform","score"));
		
		declarer.declareStream(Constants.item_category_stream, 
				new Fields("topic","bid","imp_date","category_id","name","level","father_id"));
		
		declarer.declareStream(Constants.action_weight_stream, 
				new Fields("topic","bid","imp_date","type_id","weight"));
		
		declarer.declareStream(Constants.actions_stream, 
				new Fields("topic","bid","qq","uid","imei","item_id","lbs_info","ad_pos","action_time","action_type","action_result"));
		
		
		
		declarer.declareStream("filter_data", new Fields(""));
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
	}
}
