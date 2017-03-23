package kafka.testkit

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import stlabs.kafka.testkit.KafkaServer

trait KafkaTestServer extends FlatSpec with Matchers with BeforeAndAfterAll {
  val kafkaServer = new KafkaServer()

  override def beforeAll(): Unit = {
    kafkaServer.startup()
  }

  override def afterAll(): Unit = {
    kafkaServer.close()
  }

}
