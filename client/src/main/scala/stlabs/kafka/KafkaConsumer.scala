package stlabs.kafka

import java.util.Properties

import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetResetStrategy, KafkaConsumer => JKafkaConsumer}
import org.apache.kafka.common.serialization.Deserializer
import stlabs.kafka.TypesafeConfigExtensions._

object KafkaConsumer {

  object Conf {
    def apply[K, V](keyDeserializer: Deserializer[K],
                    valueDeserializer: Deserializer[V],
                    bootstrapServers: String = "localhost:9092",
                    groupId: String = "test",
                    enableAutoCommit: Boolean = true,
                    autoCommitInterval: Int = 1000,
                    sessionTimeoutMs: Int = 30000): Conf[K, V] = {
      val props = new Properties()
      props.put("bootstrap.servers", bootstrapServers)
      props.put("group.id", groupId)
      props.put("enable.auto.commit", enableAutoCommit.toString)
      props.put("auto.commit.interval.ms", autoCommitInterval.toString)
      props.put("session.timeout.ms", sessionTimeoutMs.toString)
      apply(props, keyDeserializer, valueDeserializer)
    }

    def apply[K, V](config: Config, keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]): Conf[K, V] =
      apply(config.toProperties, keyDeserializer, valueDeserializer)
  }

  case class Conf[K, V](props: Properties, keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]) {

    def withAutoOffsetReset(strategy: OffsetResetStrategy): Conf[K, V] = {
      props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, strategy.toString)
      this
    }
  }

  def apply[K, V](conf: Conf[K, V]): JKafkaConsumer[K, V] =
    new JKafkaConsumer[K, V](conf.props, conf.keyDeserializer, conf.valueDeserializer)
}