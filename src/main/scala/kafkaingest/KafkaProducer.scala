package kafkaingest

import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}

object KafkaProducer {

  def apply[K, V](props: Properties, keySerializer: Serializer[K], valueSerializer: Serializer[V]) = {
    new KafkaProducer[K, V](props, keySerializer, valueSerializer)
  }

  def apply[K, V](bootstrapServers: String = "localhost:9092",
                  acks: String = "all",
                  retries: Int = 0,
                  batchSize: Int = 16384,
                  lingerMs: Int = 1,
                  bufferMemory: Int = 33554432,
                  keySerializer: Serializer[String] = new StringSerializer(),
                  valueSerializer: Serializer[String] = new StringSerializer()) = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("ack", acks)
    props.put("retries", retries.toString)
    props.put("batch.size", batchSize)
    props.put("linger.ms", lingerMs.toString)
    props.put("buffer.memory", bufferMemory.toString)
    new KafkaProducer(props, keySerializer, valueSerializer)
  }
  
}
