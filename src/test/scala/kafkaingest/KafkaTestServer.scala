package kafkaingest

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

trait KafkaTestServer extends FlatSpec with Matchers with BeforeAndAfterAll {
  val kafkaServer = new KafkaServer()

  override def beforeAll(): Unit = {
    kafkaServer.startup()
  }

  override def afterAll(): Unit = {
    kafkaServer.close()
  }

}
