name := "reactive-kafka"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % versions.akka,
  "com.softwaremill.reactivekafka" % "reactive-kafka-core_2.11" % "0.10.0"
)