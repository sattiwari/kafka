package kafkaingest

import java.util.Properties

import com.typesafe.config.Config
import kafka.controller.Callbacks.CallbackBuilder
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}
import org.slf4j.LoggerFactory

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

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
    props.put("acks", acks)
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

  def send(topic: String, value: V, key: Option[K] = None): Future[RecordMetadata] = {
    val promise = Promise[RecordMetadata]()
    log.info(s"sending message to topic $topic, key ${key.toString} value ${value.toString}")
    producer.send(kafkaMessage(topic, key, value), producerCallback(promise))
    promise.future
  }

  def sendWithCallback(topic: String, value: V, key: Option[K] = None)(callback: Try[RecordMetadata] => Unit): Unit = {
    log.info(s"sending message to the topic ${topic} key ${key.toString} value ${value.toString}")
    producer.send(kafkaMessage(topic, key, value), producerCallback(callback))
  }

  def flush(): Unit = {
    producer.flush()
  }

  private def kafkaMessage(topic: String, key: Option[K], value: V): ProducerRecord[K, V] = {
    val k: K = key.getOrElse(null.asInstanceOf[K])
    new ProducerRecord(topic, k, value)
  }

  def close(): Unit = {
    log.debug("closing producer")
    producer.close()
  }

  private def producerCallback(promise: Promise[RecordMetadata]): Callback = {
    producerCallback(result => promise.complete(result))
  }

  private def producerCallback(callback: Try[RecordMetadata] => Unit): Callback = {
    new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        val result =
          if(exception == null) Success(metadata)
          else Failure(exception)
        callback(result)
      }
    }
  }

}
