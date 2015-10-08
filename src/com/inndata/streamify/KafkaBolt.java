package com.inndata.streamify;

/**
 * Author : Vidya
 * Date   : 01-08-2015
 * Class  : SimpleBolt.java 
 */

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class KafkaBolt implements IRichBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = -4920056542356733137L;
	private OutputCollector outputCollector;

	@SuppressWarnings("rawtypes")
	public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
		this.outputCollector = oc;
	}

	public void execute(Tuple tuple) {
		String recData = tuple.getValue(0).toString();
		System.out.println("Received Message:" + recData);
		try {
			JSONArray liveBroadcastJSONArray = new JSONArray(recData);
			for (int i=0; i < liveBroadcastJSONArray.length(); i++) {
				JSONObject item = liveBroadcastJSONArray.getJSONObject(i);
                String class_name = item.getString("class_name");
                String id = item.getString("id");
                String created_at = item.getString("created_at");
                String updated_at = item.getString("updated_at");
                String user_id = item.getString("user_id");
                String user_display_name = item.getString("user_display_name");
                String profile_image_url = item.getString("profile_image_url");
                String state = item.getString("state");
                Boolean is_locked = item.getBoolean("is_locked");
                Boolean friend_chat = item.getBoolean("friend_chat");
                String start = item.getString("start");                
                Boolean has_location = item.getBoolean("has_location");
                String city = item.getString("city");
                String country = item.getString("country");
                String country_state = item.getString("country_state");
                String iso_code = item.getString("iso_code");
                int ip_lat = item.getInt("ip_lat");
                int ip_lng = item.getInt("ip_lng");
                int width = item.getInt("width");
                int height = item.getInt("height");
                String image_url = item.getString("image_url");
                String image_url_small = item.getString("image_url_small");
                String status = item.getString("status");
                Boolean available_for_replay = item.getBoolean("available_for_replay");
                Boolean featured = item.getBoolean("featured");
                int sort_score = item.getInt("sort_score");
                Boolean is_trusted = item.getBoolean("is_trusted");                
                String Message = class_name+"\t"+id+"\t"+created_at+"\t"+updated_at+"\t"+user_id+"\t"+user_display_name+"\t"+profile_image_url+"\t"+state+"\t"+is_locked+"\t"+friend_chat+"\t"+start+"\t"+has_location+"\t"+city+"\t"+country+"\t"+country_state+"\t"+iso_code+"\t"+ip_lat+"\t"+ip_lng+"\t"+width+"\t"+height+"\t"+image_url+"\t"+image_url_small+"\t"+status+"\t"+available_for_replay+"\t"+featured+"\t"+sort_score+"\t"+is_trusted;
                System.out.println("Received Message2 :" + Message);
                KakfaProducer.kafka(Message);
				//KafkaProducer.Kafka(item.toString());
			}
		} catch (JSONException e1) {
			// TODO Auto-generated catch block
			Logger.getLogger(KafkaBolt.class.getName()).log(Level.SEVERE, null, e1);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.outputCollector.ack(tuple);
	}

	public void cleanup() {
		// TODO
	}

	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		// TODO
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}