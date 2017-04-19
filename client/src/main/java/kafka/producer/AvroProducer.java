package kafka.producer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class AvroProducer {


    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        // avro serializer for key and value
        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");

        //as avro uses Schema Registry to store the avro schema, we have to specify where
        //it is located.
        props.put("schema.registry.url", ""); // Schema Registry url

        String schemaString = "{\"namespace\": \"test.avro\""+
                "\"name\" : \"Test\"," +

                "\"fields\": [" + "{\"name\": \"id\", \"type\": \"int\"},"
                + "{\"name\": \"name\", \"type\": \"string\"},"
                + "]}";

        Producer <String,GenericRecord> producer = new KafkaProducer<String,GenericRecord>(props);

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaString);

        int testId =1 ;
        while(testId < 10) {

            GenericRecord test = new GenericData.Record(schema);
            test.put("id", testId);
            test.put("name", "nameTest"+testId);

            ProducerRecord<String,GenericRecord> data = new ProducerRecord<String,GenericRecord>("test","nameTest"+testId,test);
            producer.send(data);
        }
    }
}