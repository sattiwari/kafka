package kafkaingest.testkit

import java.io.File

import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.curator.test.TestingServer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.slf4j.LoggerFactory

object KafkaServer {

  private val log = LoggerFactory.getLogger(getClass)

  private def randomAvailablePort(): Int = ???

//  default configuration used by Kafka test server
  val defaultConfig: Map[String, String] = ???

//  default config used for consuming records using KafkaServer
  val defaultConsumerConfig: Map[String, String] = ???

//  default config used for producing records using Kafka Server
  val defaultProducerConfig: Map[String, String] = ???

  private def createConfig(port: Int, zookeeperPort: Int, logDir: File, otherOptions: Map[String, String]): KafkaConfig = ???

  private def createTempDir(dirPrefix: String): File = ???

  private def deleteFile(path: File): Unit = ???

//  a startable kafka and zookeeper server. Kafka and zookeeper port is generatd by default
  final class KafkaServer( val kafkaPort: Int = KafkaServer.randomAvailablePort(),
                           val zookeeperPort: Int = KafkaServer.randomAvailablePort(),
                           kafkaConfig: Map[String, String] = KafkaServer.defaultConfig
                         ) {
    import KafkaServer._

    private val logDir = createTempDir("kafka-local")
    private val bootstrapServerAddress = "localhost:" + kafkaPort.toString

  //  zookeeper server
    private val zkServer = new TestingServer(zookeeperPort, false)

  //  build the kafka config with zookeeper connection
    private val config = createConfig(kafkaPort, zookeeperPort, logDir = logDir, kafkaConfig)

  //  kafka test server
    private val kafkaServer = new KafkaServerStartable(config)

    def startup(): Unit = ???

    def close(): Unit = ???

  //  consume records from Kafka synchronously
    def consume[Key, Value](topic: String, expectedNumOfRecords: Int, timeout: Long, keyDeserializer: Deserializer[Key], valueDeserializer: Deserializer[Value],
                            consumerConfig: Map[String, String] = defaultConsumerConfig): Seq[(Option[Key], Value)] = {

    }

  //  send records to Kafka synchronously
    def produce[Key, Value](topic: String, records: Iterable[ProducerRecord[Key, Value]], keySerializer: Serializer[Key],
                            valueSerializer: Serializer[Value], producerConfig: Map[String, String] = defaultProducerConfig): Unit = {

    }

  }

}
