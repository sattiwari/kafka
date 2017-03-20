package kafkaingest

import java.net.ServerSocket

import org.apache.curator.test.TestingServer
import org.slf4j.LoggerFactory

object KafkaServer {


  private def choosePort() = choosePorts(1).head

  private def choosePorts(count: Int): List[Int] = {
    val sockets = for {
      i <- 0 until count
    } yield new ServerSocket(i)

    val socketList = sockets.toList
    val ports = socketList.map(_.getLocalPort)
    socketList.map(_.close())
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

  

  def startup() = ???

  def close() = ???

}