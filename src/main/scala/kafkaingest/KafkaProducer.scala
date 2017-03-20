package kafkaingest

import java.util.Properties

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}
import org.slf4j.LoggerFactory

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
    props.put("batch.size", batchSize.toString)
    props.put("linger.ms", lingerMs.toString)
    props.put("buffer.memory", bufferMemory.toString)
    new KafkaProducer(props, keySerializer, valueSerializer)
  }
  
}

class KafkaProducer[K, V](props: Properties, keySerializer: Serializer[K], valueSerializer: Serializer[V]) {
  import org.apache.kafka.clients.producer.{KafkaProducer => JKafkaProducer}

  val log = LoggerFactory.getLogger(getClass)

  val producer = new JKafkaProducer[K, V](props, keySerializer, valueSerializer)

  def send(topic: String, key: K, value: V): Unit = {
    log.info(s"sending message to topic $topic, key ${key.toString} value ${value.toString}")
    producer.send(kafkaMessage(topic, key, value))
  }

  private def kafkaMessage(topic: String, key: K, value: V): ProducerRecord[K, V] = {
    new ProducerRecord(topic, key, value)
  }
}