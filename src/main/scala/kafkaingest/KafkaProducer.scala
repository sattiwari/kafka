package kafkaingest

import java.util.Properties

import com.typesafe.config.Config
import kafka.controller.Callbacks.CallbackBuilder
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}
import org.slf4j.LoggerFactory

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}
import kafkaingest.TypesafeConfigExtensions._
import org.apache.kafka.clients.producer.{KafkaProducer => JKafkaProducer}

object KafkaProducer {

  def apply[K, V](producer: JKafkaProducer[K, V]) = new KafkaProducer(producer)

  def apply[K, V](props: Properties) = apply(new JKafkaProducer[K, V](props))

  def apply[K, V](props: Properties, keySerializer: Serializer[K], valueSerializer: Serializer[V]) = {
    apply(new JKafkaProducer[K, V](props, keySerializer, valueSerializer))
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
    apply(props, keySerializer, valueSerializer)
  }

  def apply[K, V](config: Config) = {
    apply(config.toProperties)
  }
  
}

class KafkaProducer[K, V](val producer: JKafkaProducer[K, V]) {
  private val log = LoggerFactory.getLogger(getClass)

  def send(record: ProducerRecord[K, V]): Future[RecordMetadata] = {
    val promise = Promise[RecordMetadata]()
    logSend(record)
    producer.send(record, producerCallback(promise))
    promise.future
  }

  def sendWithCallback(record: ProducerRecord[K, V])(callback: Try[RecordMetadata] => Unit): Unit = {
    logSend(record)
    producer.send(record, producerCallback(callback))
  }

  def flush(): Unit = {
    producer.flush()
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

  private def logSend(record: ProducerRecord[K, V]): Unit = {
    val key = Option(record.key()).map(_.toString).getOrElse("null")
    log.info(s"sending message to the topic ${record.topic()} key ${key} value ${record.value().toString}")
  }

}
