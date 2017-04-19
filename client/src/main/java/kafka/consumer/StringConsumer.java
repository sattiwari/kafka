package kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class StringConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        // 3 mandatory parameters: bootstrap.servers, key.deserializer, value.deserializer
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // not mandatory but we can consider it is.
        props.put("group.id", "TestGroup");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(props);

        consumer.subscribe(Collections.singletonList("test-sample"));

//        subscribe to all test topics
        //consumer.subscribe("test.*");

//        the poll loop
        while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records)
				System.out.printf("offset = %d, key = %s, value = %s",
						record.offset(), record.key(), record.value());
		}

//        while(true){
//            ConsumerRecords<String, String> res1 = consumer.poll(100);
//            for(TopicPartition p : res1.partitions()){
//                List<ConsumerRecord<String, String>> pRes = res1.records(p);
//                for(ConsumerRecord<String, String>r:pRes){
//                    r.value();
//                }
//            }
//        }

    }

}
