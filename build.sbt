lazy val commonSettings = Seq(
  organization := "com.stlabs",
  scalaVersion := "2.11.8"
)

lazy val scalaKafkaClient = project.in(file("client"))
  .settings(commonSettings: _*)

lazy val kafkaTestkit = project.in(file("testkit")).dependsOn(scalaKafkaClient % "test")
  .settings(commonSettings: _*)

lazy val scalaKafkaClientAkka = project.in(file("akka")).dependsOn(scalaKafkaClient).dependsOn(kafkaTestkit % "test")
  .settings(commonSettings: _*)

lazy val reactiveKafka = project.in(file("reactive-kafka"))
  .settings(commonSettings: _*)