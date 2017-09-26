package kafka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * Kafka生产者
 * 
 * @author: hshe-161202
 * @create date: 2017年8月9日
 *
 */
public class KafkaProducer {

	public static void main(String[] args) {
		
		Properties props = new Properties();
		
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		
		props.put("metadata.broker.list", "12.99.106.143:9091,12.99.106.143:9092");
		
		Producer<Integer, String> producer = new Producer<Integer, String>(new ProducerConfig(props));
		
		String topic = "kafkatest";
		
		File file = new File("D:/track-log.txt");
		BufferedReader reader = null;
		
		try {
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			int line = 1;
			while((tempString = reader.readLine()) != null) {
				producer.send(new KeyedMessage<Integer, String>(topic,line + "---" + tempString));
				System.out.println("Success send [" + line + "] message ..");
				line ++;
			}
			reader.close();
			System.out.println("Total send [" + line + "] meaasges ..");
			
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			if(reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		producer.close();
	}
}
