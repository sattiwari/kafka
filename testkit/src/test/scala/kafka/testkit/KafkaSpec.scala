package kafka.testkit

import org.scalatest.{FlatSpec, Matchers}

//class KafkaSpec extends FlatSpec with Matchers {
//
//  "Kafka producer" should "produce" in {
//    val producer = KafkaProducer[String, String]()
//    (1 to 10).foreach(i => producer.send("test", "abc"))
//  }
//
//  "Kafka consumer" should "consume" in {
//    val consumer = KafkaConsumer[String, String]()
//
//    (1 to 10).foreach(i => {
//      println("consume")
//      consumer.consume("test"){ (k, v) =>
//        println(s"k: $k, v: $v")
//      }
//    })
//
//    consumer.close
//  }
//}
