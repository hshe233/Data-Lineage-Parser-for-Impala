package kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import util.PropertyFileUtil;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * Kafka消费者
 * 
 * @author: hshe-161202
 * @create date: 2017年8月10日
 * 
 */
public class KafkaConsumer {
	
	public ConsumerIterator<byte[], byte[]> iterator(String topic) {
		
		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

		topicCountMap.put(topic, new Integer(1));
		
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

		KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);

		ConsumerIterator<byte[], byte[]> it = stream.iterator();

		return it;
	}

	private static ConsumerConfig createConsumerConfig() {

		Properties props = new Properties();
		props.put("group.id", PropertyFileUtil.getProperty("group.id"));
		props.put("zookeeper.connect", PropertyFileUtil.getProperty("zookeeper.connect"));
		props.put("zookeeper.session.timeout.ms", PropertyFileUtil.getProperty("zookeeper.session.timeout.ms"));
		props.put("zookeeper.connection.timeout.ms", PropertyFileUtil.getProperty("zookeeper.connection.timeout.ms"));
		props.put("zookeeper.sync.time.ms", PropertyFileUtil.getProperty("zookeeper.sync.time.ms"));
		props.put("auto.commit.interval.ms", PropertyFileUtil.getProperty("auto.commit.interval.ms"));
		props.put("rebalance.backoff.ms", PropertyFileUtil.getProperty("rebalance.backoff.ms"));
		props.put("rebalance.max.retries", PropertyFileUtil.getProperty("rebalance.max.retries"));
		props.put("fetch.message.max.bytes", PropertyFileUtil.getProperty("fetch.message.max.bytes"));

		return new ConsumerConfig(props);
	}

}
