package io.github.hengyunabc.metrics.test;

import java.io.IOException;

import io.github.hengyunabc.metrics.MessageListener;
import io.github.hengyunabc.metrics.MetricsKafkaConsumer;

public class MetricsKafkaConsumerSample {

	String zookeeper;
	String topic;
	String group;

	public static void main(String[] args) throws IOException {

		String zookeeper = "localhost:5181";
		String topic = "test-kafka-reporter";
		String group = "consumer-test";

		MetricsKafkaConsumer consumer = new MetricsKafkaConsumer();

		consumer.setZookeeper(zookeeper);
		consumer.setTopic(topic);
		consumer.setGroup(group);
		consumer.setMessageListener(new MessageListener() {

			@Override
			public void onMessage(String message) {
				System.err.println(message);
			}
		});
		consumer.init();

		consumer.destroy();
	}
}
