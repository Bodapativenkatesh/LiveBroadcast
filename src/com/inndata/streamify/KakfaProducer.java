package com.inndata.streamify;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class KakfaProducer {
	public static void kafka(String Message) throws InterruptedException, ExecutionException {
		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class","kafka.producer.DefaultPartitioner");
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		producer.send(new KeyedMessage<String, String>("livebroadcast",Message));
		producer.close();
		
	}
		
	}

