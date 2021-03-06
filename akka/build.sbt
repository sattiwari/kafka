name := "kafka-client-akka"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % versions.akka,
  "com.typesafe" % "config" % "1.3.0",

  "org.apache.kafka" % "kafka-clients" % versions.kafka,
  "org.slf4j" % "slf4j-api" % versions.slf4j,
  "org.slf4j" % "log4j-over-slf4j" % versions.slf4j,
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,

  //Test deps
  "org.scalatest" % "scalatest_2.11" % versions.scalaTest % "test",
  "com.typesafe.akka" % "akka-testkit_2.11" % versions.akka % "test"
)