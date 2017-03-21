package kafkaingest

import org.slf4j.LoggerFactory

class KafkaIntSpec extends KafkaTestServer {
  val log = LoggerFactory.getLogger(getClass)

  "Integration test" should "test" in {
    val kafkaPort = kafkaServer.kafkaPort

    log.info(s"zk: ${kafkaServer.zkConnect}")
    log.info(s"kafka server: ${kafkaServer}")
    log.info(s"kafka port: ${kafkaServer.kafkaPort}")

    val consumer = KafkaConsumer[String, String](bootstrapServers = "localhost:" + kafkaPort)
    val producer = KafkaProducer[String, String](bootstrapServers = "localhost:" + kafkaPort)

    var count = 0

    log.info("!!::" + count)
    producer.send("test", "a", "1")
    producer.send("test", "a", "1")
    producer.send("test", "a", "1")
    producer.flush()
    log.info("!!!!!!!!")
    Thread.sleep(20000)
    consumer.consume("test") { (_, _) => count += 1 }
    consumer.consume("test") { (_, _) => count += 1 }
    consumer.consume("test") { (_, _) => count += 1 }
    consumer.consume("test") { (_, _) => count += 1 }
    consumer.consume("test") { (_, _) => count += 1 }
    log.info("!!" + count)


    assert(count == 3)
    consumer.close
  }

}
