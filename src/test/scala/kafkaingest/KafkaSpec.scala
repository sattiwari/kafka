package kafkaingest

import org.scalatest.{FlatSpec, Matchers}

class KafkaSpec extends FlatSpec with Matchers {

  "Kafka producer" should "produce" in {
    val producer = KafkaProducer[String, String]()
    (1 to 10).foreach(i => producer.send("test", i.toString, "abc"))
  }

  "Kafka consumer" should "consume" in {

  }



}
