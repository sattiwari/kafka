package com.stlabs.kafka.akka

import java.util.{Collection => JCollection}

import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._

object TrackPartitions {
  def apply(consumer: KafkaConsumer[_, _]): TrackPartitions = new TrackPartitions(consumer)
}

class TrackPartitions(consumer: KafkaConsumer[_, _]) extends ConsumerRebalanceListener {

  var offsets = Map[TopicPartition, Long]()

  def clearOffsets(): Unit = offsets.clear()

  override def onPartitionsRevoked(partitions: JCollection[TopicPartition]): Unit =
    partitions.foreach { partition =>
      offsets += (partition -> consumer.position(partition))
    }

  override def onPartitionsAssigned(partitions: JCollection[TopicPartition]): Unit =
    for {
      partition <- partitions
      offset <- offsets.get(partition)
    } consumer.seek(partition, offset)

}
