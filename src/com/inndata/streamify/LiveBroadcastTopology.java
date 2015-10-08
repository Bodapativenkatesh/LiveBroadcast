package com.inndata.streamify;

/**
 * Author : Vidya
 * Date   : 01-08-2015
 * Class  : SimpleTopology.java 
 */

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class LiveBroadcastTopology {
	public static void main(String[] args) throws InterruptedException {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("LiveBroadcastSpout", new LiveBroadcastSpout());
		topologyBuilder.setBolt("KafkaBolt", new KafkaBolt(), 2).setNumTasks(4).shuffleGrouping("LiveBroadcastSpout");
		Config config = new Config();
		config.setDebug(true);
		config.setNumWorkers(2);
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("KafkaTopology", config, topologyBuilder.createTopology());
		Thread.sleep(2000);
	}
}