package kafkaingest

import java.util.Properties

import com.typesafe.config.Config
import kafkaingest.TypesafeConfigExtensions._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}
import org.slf4j.LoggerFactory

object KafkaConsumer {

  def apply[K, V](props: Properties, keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]) = {
    new KafkaConsumer[K, V](props, keyDeserializer, valueDeserializer)
  }

  def apply[K, V](bootstrapServers: String = "localhost:9092",
                  groupId: String = "test",
                  enableAutoCommit: Boolean = true,
                  autoCommitInterval: Int = 1000,
                  sessionTimeoutMs: Int = 30000,
                  keyDeserializer: Deserializer[String] = new StringDeserializer(),
                  valueDeserializer: Deserializer[String] = new StringDeserializer()) = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("group.id", groupId)
    props.put("enable.auto.commit", enableAutoCommit.toString)
    props.put("auto.commit.interval.ms", autoCommitInterval.toString)
    props.put("session.timeout.ms", sessionTimeoutMs.toString)
    new KafkaConsumer(props, keyDeserializer, valueDeserializer)
  }

  def apply[K, V](config: Config, keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]): KafkaConsumer[K, V] = {
    new KafkaConsumer(config.toProperties, keyDeserializer, valueDeserializer)
  }

}

class KafkaConsumer[K, V](props: Properties, keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]) {
  import org.apache.kafka.clients.consumer.{KafkaConsumer => JKafkaConsumer}

  import scala.collection.JavaConversions._

  val log = LoggerFactory.getLogger(getClass)

  val consumer = new JKafkaConsumer[K, V](props, keyDeserializer, valueDeserializer)

  def subscribe(topics: List[String]): Unit = consumer.subscribe(topics)

  def consume(timeout: Long) = {
    consumer.poll(timeout)
  }

  def close = {
    log.debug("closing consumer")
    consumer.close()
  }


  private def kafkaMessage(topic: String, key: K, value: V): ProducerRecord[K, V] = {
    new ProducerRecord(topic, key, value)
  }
}
