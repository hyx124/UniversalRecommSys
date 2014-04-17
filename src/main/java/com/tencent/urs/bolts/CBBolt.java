package com.tencent.urs.bolts;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.ref.SoftReference;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import DataProcessor.NewsProcessor;
import NewsApp.Newsapp.NewsAttr;
import NewsApp.Newsapp.NewsCategory;
import NewsApp.Newsapp.NewsIndex;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.tencent.monitor.MonitorTools;
import com.tencent.streaming.commons.bolts.config.AbstractConfigUpdateBolt;
import com.tencent.streaming.commons.spouts.tdbank.Output;
import com.tencent.tde.client.Result;
import com.tencent.tde.client.TairClient.TairOption;

import com.tencent.urs.asyncupdate.UpdateCallBack;
import com.tencent.urs.asyncupdate.UpdateCallBackContext;
import com.tencent.urs.tdengine.TDEngineClientFactory;
import com.tencent.urs.tdengine.TDEngineClientFactory.ClientAttr;
import com.tencent.urs.utils.Constants;
import com.tencent.urs.utils.DataCache;
import com.tencent.urs.utils.Utils;

public class CBBolt extends AbstractConfigUpdateBolt{
	private static final long serialVersionUID = 2972911860800045348L;
	private List<ClientAttr> mtClientList;	
	private MonitorTools mt;
	private DataCache<NewsApp.Newsapp.NewsAttr> itemAttrCache;

	private NewsProcessor classifier;
	private int nsCbIndexTableId;
	private int dataExpireTime;
	private int cacheExpireTime;
	private int topNum;
	private boolean debug;
	private UpdateCallBack itemIndexCallBack;	
		
	public CBBolt(String config, ImmutableList<Output> outputField) {
		super(config, outputField, Constants.config_stream);
	}

	@Override
	public void updateConfig(XMLConfiguration config) {
		nsCbIndexTableId = config.getInt("item_index_table",519);
		dataExpireTime = config.getInt("data_expiretime",30*24*3600);
		cacheExpireTime = config.getInt("cache_expiretime",3600);
		debug = config.getBoolean("debug",false);
		topNum = config.getInt("top_num",100);
	}
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector){
		super.prepare(conf, context, collector);
		updateConfig(super.config);
	
		this.mtClientList = TDEngineClientFactory.createMTClientList(conf);
		this.mt = MonitorTools.getMonitorInstance(conf);
		this.itemAttrCache = new DataCache<NewsApp.Newsapp.NewsAttr>(conf);
		this.itemIndexCallBack = new UpdateCallBack(mt, this.nsCbIndexTableId,debug);
		this.classifier = new NewsProcessor();
		try {
			this.classifier.Init("./conf/news_processor.conf");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	private static Logger logger = LoggerFactory.getLogger(CBBolt.class);

	private static void genInputPbBuilder(NewsAttr.Builder cbAppBuilder,
					String itemId,Long bigType,Long midType,Long smallType,
					com.google.protobuf.ByteString bigTypeName,
					com.google.protobuf.ByteString midTypeName, 
					com.google.protobuf.ByteString smallTypeName,
					com.google.protobuf.ByteString text){
		cbAppBuilder.setNewsId(itemId);
		
		NewsCategory.Builder bigTypeBuilder = NewsCategory.newBuilder();
		NewsCategory.Builder midTypeBuilder = NewsCategory.newBuilder();
		NewsCategory.Builder smallTypeBuilder = NewsCategory.newBuilder();
		
		bigTypeBuilder.setId(bigType);
		midTypeBuilder.setId(midType);
		smallTypeBuilder.setId(smallType);
		
		bigTypeBuilder.setLevel(0);
		midTypeBuilder.setLevel(1);
		smallTypeBuilder.setLevel(2);

		
		bigTypeBuilder.setName(bigTypeName);
		midTypeBuilder.setName(midTypeName);
		smallTypeBuilder.setName(smallTypeName);
		
		cbAppBuilder.addCategory(0,bigTypeBuilder.build());
		cbAppBuilder.addCategory(1,midTypeBuilder.build());
		cbAppBuilder.addCategory(2,smallTypeBuilder.build());
		
	
		cbAppBuilder.setTitle(text);
		cbAppBuilder.setSourceId(Long.valueOf(bigType));

	}
	
	@Override
	public void processEvent(String sid, Tuple tuple) {	
		try{
			
			String bid = tuple.getStringByField("bid");
			String topic = tuple.getStringByField("topic");
			String itemId = tuple.getStringByField("item_id");
			
			if(topic.equals(Constants.item_info_stream) ){
				String bigType = tuple.getStringByField("cate_id1");
				String midType = tuple.getStringByField("cate_id2");
				String smallType = tuple.getStringByField("cate_id3");
				String bigTypeName = tuple.getStringByField("cate_name1");
				String midTypeName = tuple.getStringByField("cate_name2");
				String smallTypeName = tuple.getStringByField("cate_name3");
				String itemTime = tuple.getStringByField("item_time");
				String text = tuple.getStringByField("text");
				
				if(!Utils.isBidValid(bid) || !Utils.isItemIdValid(itemId)){
					return;
				}
				
				ByteString bigTypeNameStr = ByteString.copyFrom(bigTypeName,"UTF-8");
				ByteString midTypeNameStr = ByteString.copyFrom(midTypeName,"UTF-8");
				ByteString smallTypeNameStr = ByteString.copyFrom(smallTypeName,"UTF-8");
				ByteString textStr = ByteString.copyFrom(text,"UTF-8");
				
	
				NewsAttr.Builder cbAppBuilder = NewsAttr.newBuilder();
				genInputPbBuilder(cbAppBuilder,itemId,Long.valueOf(bigType),Long.valueOf(midType),Long.valueOf(smallType)
						,bigTypeNameStr,midTypeNameStr,smallTypeNameStr,textStr);
				cbAppBuilder.setIndexScore(Long.valueOf(itemTime));
				cbAppBuilder.setFreshnessScore(Long.valueOf(itemTime));
				
				classifier.HierarchicalClassify(cbAppBuilder);
				
				boolean isSend = false;
				for(NewsApp.Newsapp.NewsCategory cs:cbAppBuilder.getCategoryList()){
					String tag = cs.getName().toStringUtf8();
					if(cs.getName().isEmpty() || tag.equals("")){
						continue;
					}
						
					String tagKey = Utils.spliceStringBySymbol("#", bid,tag); 		
					cbAppBuilder.setTagScore(cs.getWeight());
						
					doReIndexsCallBack(tagKey,cbAppBuilder.build());
					isSend =true;
				}
				
				Long date = Utils.getDateByTime(Long.valueOf(itemTime));
				String inTdeKey = Utils.spliceStringBySymbol("#", bid,itemId);
				if(isSend){
					logger.info("do indexs ,for item="+inTdeKey+",date="+date+",tag_count="+cbAppBuilder.getCategoryCount());
				}
				saveNewsAttrInTde(inTdeKey, cbAppBuilder.build());
			}
		}catch(Exception e){
			logger.error(e.getMessage(), e);
		}

	}

	private void saveNewsAttrInTde(String key, NewsAttr value){
		synchronized(itemAttrCache){
			itemAttrCache.set(key, new SoftReference<NewsApp.Newsapp.NewsAttr>(value), cacheExpireTime);
		}
		
		for(ClientAttr clientEntry:mtClientList ){
			TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
			try {
				Future<Result<Void>> future = 
				clientEntry.getClient().putAsync((short)nsCbIndexTableId, 
									key.getBytes(), value.toByteArray(), putopt);
				clientEntry.getClient().notifyFuture(future, this.itemIndexCallBack, 
						new UpdateCallBackContext(clientEntry,key,value.toByteArray(),putopt));
			}catch(Exception e){
				logger.error(e.getMessage(), e);
			}
			break;
		}
	}

	private void doReIndexsCallBack(String tagKey,NewsAttr newItemAttr){
		ClientAttr clientEntry = mtClientList.get(0);		
		TairOption opt = new TairOption(clientEntry.getTimeout());
		
		NewsIndex oldNewsList = null;
		try {
			Result<byte[]> res = clientEntry.getClient().get((short)nsCbIndexTableId, tagKey.getBytes("UTF-8"), opt);
			if(res.isSuccess() && res.getResult() != null){
				oldNewsList = NewsIndex.parseFrom(res.getResult());
			}
		} catch(Exception e){
		}
		
		NewsIndex.Builder newListBuilder = NewsIndex.newBuilder();
		insertNewValueToList(oldNewsList,newListBuilder,newItemAttr);
		save(tagKey, newListBuilder.build());

	}
	
	private void insertNewValueToList(NewsIndex oldNewsList, NewsIndex.Builder newListBuilder, NewsAttr newItemAttr){
		HashSet<String> alreadyIn = new HashSet<String>();
		
		Long now = System.currentTimeMillis()/1000;
		newListBuilder.setCreateTime(now);
		newListBuilder.setUpdateTime(now);
		
		if(oldNewsList != null){
			newListBuilder.setCreateTime(oldNewsList.getCreateTime());
			for(NewsAttr eachNews :oldNewsList.getNewsListList()){
				if(newListBuilder.getNewsListCount() > topNum){
					break;
				}
				
				if(Utils.getDateByTime((long) eachNews.getFreshnessScore() )<= 20140401){
					continue;
				}
				
				if(!alreadyIn.contains(newItemAttr.getNewsId()) 
						&& newItemAttr.getFreshnessScore() >= eachNews.getFreshnessScore()){
					newListBuilder.addNewsList(newItemAttr);
					alreadyIn.add(newItemAttr.getNewsId());
				}
					
				if(!alreadyIn.contains(eachNews.getNewsId()) 
						&& !eachNews.getNewsId().equals(newItemAttr.getNewsId())){

					newListBuilder.addNewsList(eachNews);
					alreadyIn.add(eachNews.getNewsId());
				}
			}
		}
			
		if(!alreadyIn.contains(newItemAttr.getNewsId())  
				&& newListBuilder.getNewsListCount() < topNum){
			newListBuilder.addNewsList(newItemAttr);
			alreadyIn.add(newItemAttr.getNewsId());
		}
	}
	
	private void save(String key, NewsIndex value){		
		for(ClientAttr clientEntry:mtClientList ){
			TairOption putopt = new TairOption(clientEntry.getTimeout(),(short)0, dataExpireTime);
			try { 
				clientEntry.getClient().put((short)nsCbIndexTableId, key.getBytes("UTF-8"), value.toByteArray(), putopt);
			} catch(Exception e){
				logger.error(e.getMessage(), e);
			}
		}
	}
	
	public static void main(String[] args) throws IOException{
		NewsProcessor test =  new NewsProcessor();
		// /Users/xiangyu/Documents/Code/storm-stream/UniversalRecommSys/lib/libTCWordSeg.so:  
		//	 no suitable image found.  Did find:  /Users/xiangyu/Documents/Code/storm-stream/UniversalRecommSys/lib/libTCWordSeg.so:
		try {
			test.Init("./conf/news_processor.conf");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		ByteString bigTypeNameStr;
		try {
			bigTypeNameStr = ByteString.copyFrom("体育","UTF-8");
			ByteString midTypeNameStr = ByteString.copyFrom("体育","UTF-8");
			ByteString smallTypeNameStr = ByteString.copyFrom("体育","UTF-8");
			ByteString textStr = ByteString.copyFrom("深圳【世界杯】托蒂落选大名单######","UTF-8");
			
			NewsAttr.Builder cbAppBuilder = NewsAttr.newBuilder();
			genInputPbBuilder(cbAppBuilder,"123",Long.valueOf("123"),Long.valueOf("123"),Long.valueOf("123")
					,bigTypeNameStr,midTypeNameStr,smallTypeNameStr,textStr);
			cbAppBuilder.setIndexScore(Long.valueOf("133"));
			cbAppBuilder.setFreshnessScore(Long.valueOf("133"));
			
			test.HierarchicalClassify(cbAppBuilder);
			
			for(NewsApp.Newsapp.NewsCategory cs:cbAppBuilder.getCategoryList()){
				String tag = cs.getName().toStringUtf8();
				if(cs.getName().isEmpty() || tag.equals("")){
					continue;
				}
					
				System.out.print(tag);
			}
		} catch (UnsupportedEncodingException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return;
		}
	}
	
}