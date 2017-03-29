package com.stlabs.kafka.akka

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.stlabs.kafka.akka.KafkaConsumerActor.{Poll, Records, Subscribe}
import com.typesafe.config.ConfigFactory
import kafka.testkit.KafkaTestServer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import stlabs.kafka.{KafkaProducer, KafkaProducerRecord}

import scala.concurrent.duration._
import scala.util.Random

class KafkaConsumerActorSpec(system: ActorSystem) extends TestKit(system) with KafkaTestServer with ImplicitSender {

  val log = LoggerFactory.getLogger(getClass)

  def this() = this(ActorSystem("MySpec"))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def consumerConf(topic: String): KafkaConsumerActor.Conf[String, String] = {
    KafkaConsumerActor.Conf(
      ConfigFactory.parseString(
        s"""
           |bootstrap.servers = "localhost:${kafkaServer.kafkaPort}",
           |group.id = "test",
           |key.deserializer = "org.apache.kafka.serialization.StringDeserializer",
           |value.deserializer = "org.apache.kafka.serialization.StringDeserializer"
         """.stripMargin), List(topic))
  }

  "KafkaConsumerActor in self managed offsets mode" should "consume a sequence of messages" in {

    val kafkaPort = kafkaServer.kafkaPort
    val topic = randomString(5)

    log.info(s"using topic [$topic] and [$kafkaPort]")

    val producer = KafkaProducer(new StringSerializer(), new StringSerializer(), bootstrapServers = "localhost:" + kafkaPort)
    val consumer = system.actorOf(KafkaActor.consumer(consumerConf(topic), testActor), "consumer")

    producer.send(KafkaProducerRecord(topic, None, "value"))
    producer.flush()

    log.info(s"ready to poll")

    consumer ! Subscribe()

    Thread.sleep(5000)
    producer.send(KafkaProducerRecord(topic, None, "value"))

    Thread.sleep(5000)
    producer.send(KafkaProducerRecord(topic, None, "value"))

    implicit val timeout: FiniteDuration = 120.seconds
    expectMsgClass(timeout, classOf[Records[String, String]])

  }

  def randomString(length: Int): String = new Random().alphanumeric.take(length).mkString

}
