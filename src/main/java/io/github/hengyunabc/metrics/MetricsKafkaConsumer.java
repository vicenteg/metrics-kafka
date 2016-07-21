package io.github.hengyunabc.metrics;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.kafka.clients.consumer.KafkaConsumer;

public class MetricsKafkaConsumer {
	private static final Logger logger = LoggerFactory.getLogger(MetricsKafkaConsumer.class);
	String zookeeper;
	String group;
	String topic;

	int threadNumber = 1;

	int zookeeperSessionTimeoutMs = 4000;
	int zookeeperSyncTimeMs = 2000;
	int autoCommitIntervalMs = 1000;
  int pollTimeout = 5000;

	MessageListener messageListener;

	private KafkaConsumer<String,String> consumer;

	@SuppressWarnings("rawtypes")
	public void init() {
		Properties props = new Properties();
    JsonParser parser = new JsonParser();

    props.put("zookeeper.connect", zookeeper);
		props.put("group.id", group);
		props.put("zookeeper.session.timeout.ms", "" + zookeeperSessionTimeoutMs);
		props.put("zookeeper.sync.time.ms", "" + zookeeperSyncTimeMs);
		props.put("auto.commit.interval.ms", "" + autoCommitIntervalMs);

		consumer = new KafkaConsumer<String,String>(props,
				new org.apache.kafka.common.serialization.StringDeserializer(),
				new org.apache.kafka.common.serialization.StringDeserializer());

    consumer.subscribe(Collections.singletonList(topic));
    ConsumerRecords<String, String> records = consumer.poll(pollTimeout);

    for (ConsumerRecord<String, String> record : records) {
      JsonObject o = parser.parse(record.value()).getAsJsonObject();
      messageListener.onMessage(o.getAsString());
    }
	}

	public void destroy() {
		try {
			if (consumer != null) {
				consumer.close();
			}
		} finally {

		}
	}

	public String getZookeeper() {
		return zookeeper;
	}

	public void setZookeeper(String zookeeper) {
		this.zookeeper = zookeeper;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getThreadNumber() {
		return threadNumber;
	}

	public void setThreadNumber(int threadNumber) {
		this.threadNumber = threadNumber;
	}

	public int getZookeeperSessionTimeoutMs() {
		return zookeeperSessionTimeoutMs;
	}

	public void setZookeeperSessionTimeoutMs(int zookeeperSessionTimeoutMs) {
		this.zookeeperSessionTimeoutMs = zookeeperSessionTimeoutMs;
	}

	public int getZookeeperSyncTimeMs() {
		return zookeeperSyncTimeMs;
	}

	public void setZookeeperSyncTimeMs(int zookeeperSyncTimeMs) {
		this.zookeeperSyncTimeMs = zookeeperSyncTimeMs;
	}

	public int getAutoCommitIntervalMs() {
		return autoCommitIntervalMs;
	}

	public void setAutoCommitIntervalMs(int autoCommitIntervalMs) {
		this.autoCommitIntervalMs = autoCommitIntervalMs;
	}

	public MessageListener getMessageListener() {
		return messageListener;
	}

	public void setMessageListener(MessageListener messageListener) {
		this.messageListener = messageListener;
	}
}
