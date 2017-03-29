package com.stlabs.kafka.akka

import akka.actor.{Actor, ActorLogging, ActorRef}
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

    def isNewerThan(that: Offsets): Boolean =
      offsets.forAllOffsets(that)(_ > _)

    def values: Seq[V] = records.toList.map(_.value())

  }

  sealed trait Command
  case class Reset(offsets: Offsets) extends Command
  case class ConfirmOffsets(offsets: Offsets, commit: Boolean = false) extends Command

  case object Poll extends Command

  case class Conf[K, V](consumerConfig: Config, topics: List[String])

  object State {
    def empty[K, V]: State[K, V] = State(None, None)
  }

  case class State[K, V](onTheFly: Option[Records[K, V]], buffer: Option[Records[K, V]]) {

    def offsetsAreOld(offsets: Offsets): Boolean =
      onTheFly.exists(_ isNewerThan offsets)

    def isFull: Boolean = onTheFly.nonEmpty && buffer.nonEmpty
  }

  val defaultConsumerConfig: Config =
    ConfigFactory.parseMap(Map(
      "enable.auto.commit" -> "false"
    ))

}

class KafkaConsumerActor[K: TypeTag, V: TypeTag](conf: KafkaConsumerActor.Conf[K, V], nextActor: ActorRef) extends Actor with ActorLogging {
  import KafkaConsumerActor._

  private val kafkaConfig = defaultConsumerConfig.withFallback(conf.consumerConfig)
  private val consumer = KafkaConsumer[K, V](kafkaConfig)
  private val trackPartitions = TrackPartitions(consumer)

  consumer.subscribe(conf.topics, trackPartitions)

  log.info(s"starting consumer for topics [${conf.topics.mkString(",")}]")

  private var state: State[K, V] = State.empty

  override def receive: Receive = {

    case Reset(offsets) =>
      log.info(s"resetting offsets ${conf.topics}, ${offsets}")
      trackPartitions.offsets = offsets.offsetsMap
      state = State.empty
      schedulePoll()

    case Poll if state.isFull =>
      log.info(s"buffer is full, not gonna poll ${conf.topics}")

    case Poll =>
      log.info(s"poll")
      poll() foreach { records =>
        state = appendRecords(state, records)
      }

    case ConfirmOffsets(offsets, commit) =>
      log.info(s"confirm offsets ${conf.topics}, ${offsets}")
      if(state.offsetsAreOld(offsets)) {
        log.info(s"offsets are old, resending ${conf.topics}")
        resendCurrentRecords(state)
      } else {
        log.info(s"offsets are new ${conf.topics}")
        state = sendNextRecords(state)
        if(commit) {
          commitOffsets(offsets)
        }
      }
  }

  private def sendNextRecords(currentState: State[K, V]): State[K, V] = {
    currentState.buffer.orElse(poll()) match {
      case Some(records) =>
        log.debug(s"got next records, sending")
        sendRecords(records)
        currentState.copy(onTheFly = Some(records), buffer = poll())

      case None =>
        log.debug(s"no records available")
        State.empty
    }
  }

  private def resendCurrentRecords(currentState: State[K, V]) = {
    currentState.onTheFly.foreach { rs =>
      log.debug(s"resending current records")
      sendRecords(rs)
    }
  }

  private def appendRecords(currentState: State[K, V], records: Records[K, V]) = {
    (currentState.onTheFly, currentState.buffer) match {
      case (None, None) =>
        sendRecords(records)
        State(Some(records), None)

      case (Some(_), None) =>
        currentState.copy(buffer = Some(records))

      case _ =>
        currentState
    }
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
    context.system.scheduler.scheduleOnce(500 millis, self, Poll)(context.dispatcher)
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
