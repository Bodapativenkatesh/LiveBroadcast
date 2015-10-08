package com.inndata.streamify;

/**
 * Author : Vidya
 * Date   : 01-08-2015
 * Class  : SimpleSpout.java 
 */

import java.util.Map;
import java.util.logging.*;
import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.JSONException;
import org.json.JSONObject;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class LiveBroadcastSpout implements IRichSpout{

	private static final long serialVersionUID = -8315724725101034741L;
	private SpoutOutputCollector spoutOutputCollector;
	private boolean  completed = false;
	private static int i = 0;

	public void open(@SuppressWarnings("rawtypes") Map map, TopologyContext tc, SpoutOutputCollector soc) {      
		this.spoutOutputCollector = soc;
	}

	public void close() {
		// Todo
	}

	public void activate() {
		// Todo
	}

	public void deactivate() {
		// Todo
	}

	public void nextTuple() {
		if(!completed)
		{
			if(i < 100)
			{
				String myURL = "https://api.periscope.tv/api/v2/liveBroadcastFeed";
				JSONObject json = new JSONObject();
				try {
					json.put("cookie","V5olIDEyMDQ2MDXeyvnV-ia-nE1eag1hzp8G_AM5ZEt-PHfwxMEhWnzpCQ==");
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					Logger.getLogger(LiveBroadcastSpout.class.getName()).log(Level.SEVERE, null, e);
				}    
				CloseableHttpClient httpClient = HttpClientBuilder.create().build();
				try {
					HttpPost request = new HttpPost(myURL);
					StringEntity params = new StringEntity(json.toString());
					request.addHeader("content-type", "application/json");
					request.setEntity(params);
					HttpResponse response = httpClient.execute(request);
					HttpEntity resEntity = response.getEntity();
					if (resEntity != null) {
						String responseBody = EntityUtils.toString(resEntity);
						if( isValidJson(responseBody)){
							System.out.println("hi Kafka:"+responseBody);
							this.spoutOutputCollector.emit(new Values(responseBody), responseBody);
						} else {
							//Do not send anything
							System.out.println("hi Kafka INVALID:"+responseBody);
							this.spoutOutputCollector.emit(new Values(responseBody), responseBody);
						}
					}
					EntityUtils.consume(resEntity);
				} catch (Exception ex) {
					// handle exception here
					Logger.getLogger(LiveBroadcastSpout.class.getName()).log(Level.SEVERE, null, ex);
				}
				try {
					httpClient.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					Logger.getLogger(LiveBroadcastSpout.class.getName()).log(Level.SEVERE, null, e);
				}
			}
			else
			{  
				completed = true;
			}
		}
		else
		{
			try {
				Thread.sleep(2000);
			} catch (InterruptedException ex) {
				Logger.getLogger(LiveBroadcastSpout.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
	}

	private static boolean isValidJson(String jsonStr) {
		boolean isValid = false;
		try {
			@SuppressWarnings("unused")
			JSONObject jsonObj = new JSONObject(jsonStr);
			isValid = true;
		} catch (Exception je) {
			isValid = false;
		}
		return isValid;
	}

	public void ack(Object o) {
		System.out.println("\n\n OK : " + o);
	}

	public void fail(Object o) {
		System.out.println("\n\n Fail : " + o);
	}

	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		ofd.declare(new Fields("word"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}