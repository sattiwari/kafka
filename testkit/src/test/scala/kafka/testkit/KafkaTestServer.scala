package kafka.testkit

import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import stlabs.kafka.testkit.KafkaServer

trait KafkaTestServer extends FlatSpecLike with Matchers with BeforeAndAfterAll {
  val kafkaServer = new KafkaServer()

  override def beforeAll(): Unit = {
    kafkaServer.startup()
  }

  override def afterAll(): Unit = {
    kafkaServer.close()
  }
}
