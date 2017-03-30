package kafka.testkit

import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.slf4j.LoggerFactory
import stlabs.kafka.KafkaConsumer.Conf
import stlabs.kafka.{KafkaConsumer, KafkaProducer, KafkaProducerRecord}

import scala.util.Random
import scala.collection.JavaConversions._

class KafkaIntSpec extends KafkaTestServer {
  val log = LoggerFactory.getLogger(getClass)

  "Kafka Client" should "send and receive" in {
    val kafkaPort = kafkaServer.kafkaPort

    log.info(s"zk: ${kafkaServer.zkConnect}")
    log.info(s"kafka server: ${kafkaServer}")
    log.info(s"kafka port: ${kafkaServer.kafkaPort}")

    val topic = randomString(5)
    val consumer = KafkaConsumer(Conf(new StringDeserializer(), new StringDeserializer(), bootstrapServers = "localhost:" + kafkaPort))
    val producer = KafkaProducer(new StringSerializer(), new StringSerializer(), bootstrapServers = "localhost:" + kafkaPort)

    consumer.subscribe(List(topic))
    val records1 = consumer.poll(1000)
    records1.count() shouldEqual 0

    log.info("Kafka producer connecting on port: [{}]", kafkaPort)
    producer.send(KafkaProducerRecord(topic, Some("key"), "value"))
    producer.flush()

    val records2 = consumer.poll(1000)
    records2.count() shouldEqual 1

    producer.close()
    consumer.close()
  }

  def randomString(length: Int) = {
    val random = new Random()
    random.alphanumeric.take(length).mkString
  }

}
