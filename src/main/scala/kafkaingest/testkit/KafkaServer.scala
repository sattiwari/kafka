package kafkaingest.testkit

import java.io.File
import java.net.ServerSocket

import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.curator.test.TestingServer
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetResetStrategy}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Random, Try}


object KafkaServer {

  private val log = LoggerFactory.getLogger(getClass)

  private def randomAvailablePort(): Int = {
    val socket = new ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }

  //  default configuration used by Kafka test server
  val defaultConfig: Map[String, String] = Map(
    KafkaConfig.BrokerIdProp -> "1",
    KafkaConfig.ReplicaSocketTimeoutMsProp -> "1500",
    KafkaConfig.ControlledShutdownEnableProp -> "true"
  )

  //  default config used for consuming records using KafkaServer
  val defaultConsumerConfig: Map[String, String] = Map(
    ConsumerConfig.GROUP_ID_CONFIG -> "test",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> OffsetResetStrategy.EARLIEST.toString.toLowerCase()
  )

  //  default config used for producing records using Kafka Server
  val defaultProducerConfig: Map[String, String] = Map(
    ProducerConfig.ACKS_CONFIG -> "all"
  )

  private def createConfig(port: Int, zookeeperPort: Int, logDir: File, otherOptions: Map[String, String]): KafkaConfig = {
    val baseConfig = Map("port" -> port.toString, "zookeeper.connect" -> ("localhost:" + zookeeperPort.toString), "log.dir" -> logDir.getAbsolutePath)
    val extendedConfig = otherOptions ++ baseConfig
    new KafkaConfig(extendedConfig.asJava)
  }

  private def createTempDir(dirPrefix: String): File = {
    val randomString = Random.alphanumeric.take(10).mkString("")
    val f = new File(System.getProperty("java.io.tmpdir"), dirPrefix + randomString)
    f.deleteOnExit()
    f
  }

  private def deleteFile(path: File): Unit = {
    if (path.isDirectory) {
      path.listFiles.foreach(deleteFile)
    }
    else {
      path.delete()
    }
  }
}

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

  def startup(): Unit = {
    log.info(s"zk connect string: ${zkServer.getConnectString}")
    zkServer.start()
    kafkaServer.startup()
    log.info(s"started kafka on port ${kafkaPort}")
  }

  def close(): Unit = {
    log.info(s"stopping kafka on port ${kafkaPort}")
    kafkaServer.shutdown()
    zkServer.stop()
    Try{deleteFile(logDir)}.failed.foreach(_.printStackTrace())
  }

//  consume records from Kafka synchronously
  def consume[Key, Value](topic: String, expectedNumOfRecords: Int, timeout: Long, keyDeserializer: Deserializer[Key], valueDeserializer: Deserializer[Value],
                          consumerConfig: Map[String, String] = defaultConsumerConfig): Seq[(Option[Key], Value)] = {
    val extendedConfig: Map[String, Object] = consumerConfig + (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServerAddress)
    val consumer = new KafkaConsumer(extendedConfig.asJava, keyDeserializer, valueDeserializer)

    try {
      consumer.subscribe(List(topic).asJava)

      var total = 0
      val collected = ArrayBuffer.empty[(Option[Key], Value)]
      val start = System.currentTimeMillis()

      while(total <= expectedNumOfRecords && System.currentTimeMillis() < start + timeout) {
        val records = consumer.poll(100)
        val kvs = records.asScala.map(r => (Option(r.key()), r.value()))
        collected ++= kvs
        total += records.count()
      }

      if(collected.size < expectedNumOfRecords) {
        sys.error(s"did not receive correct number of records. Expected ${expectedNumOfRecords}, got ${collected.size}")
      }

      collected.toVector
    } finally {
      consumer.close()
    }
  }

//  send records to Kafka synchronously
  def produce[Key, Value](topic: String, records: Iterable[ProducerRecord[Key, Value]], keySerializer: Serializer[Key],
                          valueSerializer: Serializer[Value], producerConfig: Map[String, String] = defaultProducerConfig): Unit = {
    val extendedConfig: Map[String, Object] = producerConfig + (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServerAddress)
    val producer = new KafkaProducer(extendedConfig.asJava, keySerializer, valueSerializer)

    try {
      records.foreach( r => producer.send(r))
    } finally {
      producer.flush()
      producer.close()
    }
  }

}
