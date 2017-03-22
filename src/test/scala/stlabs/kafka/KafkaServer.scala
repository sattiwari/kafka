package stlabs.kafka

import java.net.ServerSocket
import java.util.Properties

import org.apache.curator.test.TestingServer
import org.slf4j.LoggerFactory
import kafka.server.{KafkaConfig, KafkaServerStartable}

object KafkaServer {

//  create test config for the given node id
  private def createBrokerConfig(nodeId: Int, port: Int = choosePort(), zkConnect: String,
                                 enableControlledShutdown: Boolean = true): Properties = {
    val props = new Properties()
    props.put("broker.id", nodeId.toString)
    props.put("host.name", "localhost")
    props.put("port", port.toString)
    props.put("logdir", "./target/kafka")
    props.put("zookeeper.connect", zkConnect)
    props.put("replica.socket.timeout.ms", "1500")
    props.put("controlled.shutdown.enable", enableControlledShutdown.toString)
    props
  }

  protected def brokerConfig(zkConnect: String, enableControlledShutdown: Boolean = true): ((Int, Int)) => Properties = {
    case (port, node) => createBrokerConfig(node, port, zkConnect, enableControlledShutdown)
  }

  protected def portConfig(zkConnect: String, enableControlledShutdown: Boolean = true): ((Int, Int)) => KafkaConfig = {
    brokerConfig(zkConnect, enableControlledShutdown) andThen KafkaConfig.apply
  }

  protected def randomConfigs(num: Int, zkConnect: String, enableControlledShutdown: Boolean = true): List[KafkaConfig] = {
    choosePorts(num).zipWithIndex.map(portConfig(zkConnect, enableControlledShutdown))
  }

  protected def choosePort(): Int = choosePorts(1).head

  protected def choosePorts(count: Int): List[Int] = {
    val sockets = (0 until count).map(i => new ServerSocket(i))
    val socketList = sockets.toList
    val ports = socketList.map(_.getLocalPort)
    socketList.foreach(_.close())
    ports
  }

}

class KafkaServer(val kafkaPort: Int = KafkaServer.choosePort(), val zkPort: Int = KafkaServer.choosePort()) {

  import KafkaServer._

  val log = LoggerFactory.getLogger(getClass)

  val zkConnect = "127.0.0.1:" + zkPort

//  start a zookeeper server
  val zkServer = new TestingServer(zkPort)
  log.info("zk connect: " + zkServer.getConnectString)

//  kafka test server
  val config = portConfig(zkServer.getConnectString)((kafkaPort, 1))
  val kafkaServer = new KafkaServerStartable(config)

  def startup() = {
    kafkaServer.startup()
    log.info(s"started kafka on port $kafkaPort")
  }

  def close() = {
    log.info(s"stopping kafka on port $kafkaPort")
    kafkaServer.shutdown()
    zkServer.stop()
  }

}