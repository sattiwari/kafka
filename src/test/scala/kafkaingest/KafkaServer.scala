package kafkaingest

import java.net.ServerSocket
import java.util.Properties

import org.apache.curator.test.TestingServer
import org.slf4j.LoggerFactory
import kafka.server.{KafkaConfig, KafkaServerStartable}

object KafkaServer {

  protected def kafkaConfig(zkConnectString: String) = {
    val propsI = createBrokerConfigs(1, zkConnectString).iterator
    assert(propsI.hasNext)
    val props = propsI.next()
    assert(props.containsKey("zookeeper.connect"))
    new KafkaConfig(props)
  }

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

  private def createBrokerConfigs(numConfigs: Int, zkConnect: String, enableControlledShutdown: Boolean = true): List[Properties] = {
    (choosePorts(numConfigs).zipWithIndex).map { case (port, node) =>
      createBrokerConfig(node, port, zkConnect, enableControlledShutdown)
    }
  }


  private def choosePort() = choosePorts(1).head

  private def choosePorts(count: Int): List[Int] = {
    val sockets = (0 until count).map(i => new ServerSocket(i))
    val socketList = sockets.toList
    val ports = socketList.map(_.getLocalPort)
    socketList.foreach(_.close())
    ports
  }

}

class KafkaServer {

  import KafkaServer._

  val log = LoggerFactory.getLogger(getClass)

  val zkPort = choosePort()
  val zkConnect = "127.0.0.1:" + zkPort

//  start a zookeeper server
  val zkServer = new TestingServer(zkPort)

  val config = kafkaConfig(zkServer.getConnectString)
  log.info("zk connect: " + zkServer.getConnectString)

//  kafka test server
  val kafkaServer = new KafkaServerStartable(config)
  val kafkaPort = config.port

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