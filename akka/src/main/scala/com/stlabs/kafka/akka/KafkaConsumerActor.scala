package com.stlabs.kafka.akka

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.{ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import stlabs.kafka.KafkaConsumer

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

object KafkaConsumerActor {

  case class Subscribe(offsets: Option[Offsets] = None)
  case class Confirm(offsets: Option[Offsets] = None)
  private case object Poll
  case object Unsubscribe

  case class Offsets(val offsetsMap: Map[TopicPartition, Long]) extends AnyVal {
    def get(topic: TopicPartition): Option[Long] = offsetsMap.get(topic)

    def forAllOffsets(that: Offsets)(f: (Long, Long) => Boolean): Boolean =
      offsetsMap.forall {
        case (topic, offset) => that.get(topic).forall(f(offset, _))
      }

    def toCommitMap: Map[TopicPartition, OffsetAndMetadata] =
      offsetsMap.mapValues(offset => new OffsetAndMetadata(offset))
  }

  case class Records[K: TypeTag, V: TypeTag](offsets: Offsets, records: ConsumerRecords[K, V]) {
    val keyTag = typeTag[K]
    val valueTag = typeTag[V]

    def hasType[K1: TypeTag, V2: TypeTag]: Boolean =
      typeTag[K1].tpe <:< keyTag.tpe &&
      typeTag[V2].tpe <:< valueTag.tpe

    def cast[K1: TypeTag, V2: TypeTag]: Option[Records[K1, V2]] =
      if(hasType[K1, V2]) Some(this.asInstanceOf[Records[K1, V2]])
      else None

    def values: Seq[V] = records.toList.map(_.value())
  }

  object Conf {
    def apply(config: Config): Conf = {
      val topics = config.getStringList("consumer.topics")
      apply(topics.toList)
    }
  }

  case class Conf(topics: List[String],
                 scheduleInterval: FiniteDuration = 3000.millis,
                 unconfirmedTimeout: FiniteDuration = 3.seconds,
                 bufferSize: Int = 8) {
    def withConf(config: Config): Conf = {
      this.copy(topics = config.getStringList("consumer.topics").toList)
    }
  }

  private[akka] class ClientCache[K, V](unconfirmedTimeout: FiniteDuration, maxBuffer: Int) {
    var unconfirmed: Option[Records[K, V]] = None
    var buffer = new scala.collection.mutable.Queue[Records[K, V]]()
    var deliveryTime: Option[LocalDateTime] = None

    def isFull() = buffer.size >= maxBuffer

    def bufferRecords(records: Records[K, V]) = buffer += records

    def recordsForDelivery(): Option[Records[K, V]] = {
      if(unconfirmed.isEmpty && buffer.nonEmpty) {
        val record = buffer.dequeue()
        unconfirmed = Some(record)
        deliveryTime = Some(LocalDateTime.now())
        Some(record)
      }
      else None
    }

    def getRedeliveryRecords(): Records[K, V] = {
      assert(unconfirmed.isDefined)
      deliveryTime = Some(LocalDateTime.now())
      unconfirmed.get
    }

    def confirmationTimeout(): Boolean = {
      deliveryTime match {
        case Some(time) if unconfirmed.isDefined =>
          time plus (unconfirmedTimeout.toMillis, ChronoUnit.MILLIS) isBefore (LocalDateTime.now())

        case _ =>
          false
      }
    }

    def confirm(): Unit = {
      unconfirmed = None
    }

    def reset(): Unit = {
      unconfirmed = None
      buffer.clear()
      deliveryTime = None
    }
  }

  def props[K: TypeTag, V: TypeTag](consumerConf: KafkaConsumer.Conf[K, V], actorConf: KafkaConsumerActor.Conf, nextActor: ActorRef) =
    Props(new KafkaConsumerActor[K, V](consumerConf, actorConf, nextActor))

}

class KafkaConsumerActor[K: TypeTag, V: TypeTag](consumerConf: KafkaConsumer.Conf[K, V], actorConf: KafkaConsumerActor.Conf, nextActor: ActorRef) extends Actor with ActorLogging {
  import KafkaConsumerActor._

  private val consumer = KafkaConsumer[K, V](consumerConf)
  private val trackPartitions = TrackPartitions(consumer)

  private val clientCache: ClientCache[K, V] = new ClientCache[K, V](actorConf.unconfirmedTimeout, actorConf.bufferSize)

  override def receive: Receive = {

    case Subscribe(offsets) =>
      log.info(s"subscribing to topics ${actorConf.topics}")
      consumer.subscribe(actorConf.topics, trackPartitions)
      offsets.foreach(o => trackPartitions.offsets = o.offsetsMap)
      clientCache.reset()
      schedulePoll()

    case Poll =>
      log.info(s"poll")
      if(clientCache.confirmationTimeout()) {
        log.info("message timed out, redilivering")
        sendRecords(clientCache.getRedeliveryRecords())
      }
      if(clientCache.isFull()) log.info(s"Buffers are full. Not gonna poll. ${actorConf.topics}")
      else {
        poll() foreach { records =>
          clientCache.bufferRecords(records)
        }
      }
      clientCache.recordsForDelivery().foreach(records => sendRecords(records))

    case Confirm(offsets0) =>
      log.info(s"confirm offsets ${actorConf.topics}, ${offsets0}")
      clientCache.confirm()
      offsets0 match {
        case Some(offsets) => commitOffsets(offsets)
        case None =>
      }
      clientCache.recordsForDelivery().foreach(records => sendRecords(records))

    case Unsubscribe =>
      log.info("unsubscribing")
      consumer.unsubscribe()
      clientCache.reset()
  }

  private def poll(): Option[Records[K, V]] = {
    val result = Try(consumer.poll(0)) match {
      case Success(rs) if rs.count() > 0 =>
        Some(Records(currentConsumerOffsets, rs))

      case Success(rs) =>
        None

      case Failure(_: WakeupException) =>
        log.debug(s"poll was interrupted")
        None

      case Failure(ex) =>
        log.error(s"${ex} error occured while attempting to poll Kafka")
        None
    }

    if(result.isEmpty) schedulePoll()
    else pollImmediate()

    result
  }

  private def sendRecords(records: Records[K, V]) = {
    nextActor ! records
  }

  private def schedulePoll(): Unit = {
    log.info(s"scheduled poll")
    context.system.scheduler.scheduleOnce(3000 millis, self, Poll)(context.dispatcher)
  }

  private def pollImmediate(): Unit = {
    log.info(s"immediate poll")
    self ! Poll
  }

  private def currentConsumerOffsets: Offsets = {
    val offsetMap =
      consumer.assignment()
      .map(p => p -> consumer.position(p))
      .toMap
    Offsets(offsetMap)
  }

  private def commitOffsets(offsets: Offsets): Unit = {
    log.debug(s"committing offsets ${offsets}")
    consumer.commitSync(offsets.toCommitMap)
  }

  override def unhandled(message: Any): Unit = {
    super.unhandled(message)
    log.warning(s"unknown message ${message}")
  }

  override def postStop(): Unit = {
    consumer.wakeup()
    consumer.close()
  }
}
