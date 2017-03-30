package com.stlabs.kafka.akka

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.stlabs.kafka.akka.KafkaConsumerActor.{Confirm, Poll, Records, Subscribe}
import com.typesafe.config.ConfigFactory
import kafka.testkit.KafkaTestServer
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.slf4j.LoggerFactory
import stlabs.kafka.{KafkaConsumer, KafkaProducer, KafkaProducerRecord}

import scala.concurrent.duration._
import scala.util.Random

object KafkaConsumerActorSpec {

  def kafkaProducer(kafkaPort: Int): KafkaProducer[String, String] =
    KafkaProducer(new StringSerializer(), new StringSerializer(), bootstrapServers = "localhost:" + kafkaPort)
}

class KafkaConsumerActorSpec(system: ActorSystem) extends TestKit(system) with KafkaTestServer with ImplicitSender {

  val log = LoggerFactory.getLogger(getClass)

  def this() = this(ActorSystem("MySpec"))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  val consumerConfFromConfig: KafkaConsumer.Conf[String, String] = {
    KafkaConsumer.Conf(
      ConfigFactory.parseString(
        s"""
           | bootstrap.servers = "localhost:${kafkaServer.kafkaPort}",
           | group.id = "test"
           | enable.auto.commit = false
           | auto.offset.reset = "earliest"
        """.stripMargin), new StringDeserializer, new StringDeserializer)

  def consumerConf(topic: String): KafkaConsumerActor.Conf[String, String] = {
    KafkaConsumerActor.Conf(KafkaConsumer.Conf(
      ConfigFactory.parseString(
        s"""
           |bootstrap.servers = "localhost:${kafkaServer.kafkaPort}"
           |group.id = "test"
           |auto.offset.reset = "earliest"
         """.stripMargin), new StringDeserializer, new StringDeserializer),
      List(topic))
  }

  "KafkaConsumerActor in self managed offsets mode" should "consume a message" in {

    val kafkaPort = kafkaServer.kafkaPort
    val topic = randomString(5)

    log.info(s"using topic [$topic] and [$kafkaPort]")

    val producer = KafkaProducer(new StringSerializer(), new StringSerializer(), bootstrapServers = "localhost:" + kafkaPort)
    producer.send(KafkaProducerRecord(topic, None, "value"))
    producer.flush()

//    val consumer = system.actorOf(KafkaActor.consumer(consumerConf(topic), testActor), "consumer")
    val consumer = system.actorOf(KafkaConsumerActor.props(consumerConf(topic), testActor))
    consumer ! Subscribe()

    implicit val timeout: FiniteDuration = 30.seconds
    expectMsgClass(timeout, classOf[Records[String, String]])

    consumer ! Confirm(None)
    expectNoMsg(5.seconds)

  }

  "KafkaConsumerActor different configuration types" should "consume a message successfully" in {


  }

  "Kafka Consumer in commit mode" should "consume a sequence of messages" in {
    val kafkaPort = kafkaServer.kafkaPort
    val topic = randomString(5)
    log.info(s"using topic [${topic}] and kafka port [${kafkaPort}]")

    val producer = KafkaProducer(new StringSerializer(), new StringSerializer(), bootstrapServers = "localhost:" + kafkaPort)
    producer.send(KafkaProducerRecord(topic, None, "value"))
    producer.flush()

    val consumer = system.actorOf(KafkaConsumerActor.props(consumerConf(topic), testActor))
    consumer ! Subscribe()

    implicit val timeout: FiniteDuration = 30.seconds
    expectMsgClass(timeout, classOf[Records[String, String]])

    consumer ! Confirm()
    expectNoMsg(5.seconds)
  }

  def randomString(length: Int): String = new Random().alphanumeric.take(length).mkString

}
