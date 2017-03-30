package com.stlabs.kafka.akka

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import kafka.testkit.KafkaTestServer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.concurrent.AsyncAssertions
import org.slf4j.LoggerFactory
import stlabs.kafka.KafkaConsumer

import scala.util.Random

class KafkaConsumerActorRecoverySpec(system: ActorSystem) extends TestKit(system) with KafkaTestServer with ImplicitSender with AsyncAssertions {

  val log = LoggerFactory.getLogger(getClass)

  def this() = this(ActorSystem("myspec"))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  val consumerConf: KafkaConsumer.Conf[String, String] = {
    KafkaConsumer.Conf(
      new StringDeserializer,
      new StringDeserializer,
      bootstrapServers = s"localhost:${kafkaServer.kafkaPort}",
      groupId = "test",
      enableAutoCommit = false
    ).withAutoOffsetReset(OffsetResetStrategy.EARLIEST)
  }

  "KafkaConsumerActor with manual commit" should "recover to a commit point" in {
    val kafkaPort = kafkaServer.kafkaPort
    val topic = randomString(5)

  }

  def randomString(length: Int): String = new Random().alphanumeric.take(length).mkString
}
