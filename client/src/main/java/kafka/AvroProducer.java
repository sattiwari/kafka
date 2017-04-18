package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * Created by stiwari on 4/18/2017 AD.
 */
public class AvroProducer {
    public Properties kafkaProps;
    public KafkaProducer producer;

    public AvroProducer() {
        kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");

        kafkaProps.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        kafkaProps.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");

//        kafkaProps.put("schema.registry.url", schemaUrl);

        String topic = "customerContacts";
        int wait = 500;

        producer = new KafkaProducer<String, Customer>(kafkaProps);
    }

    public void fireAndForget(String topic, String key, String value) {

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
        try {
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendSynchronous(String topic, String key, String value) {

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
        try {
            RecordMetadata metaData = (RecordMetadata) producer.send(record).get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendAsynchronous(String topic, String key, String value){
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
        try {
            producer.send(record, new ProducerCallBack());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
