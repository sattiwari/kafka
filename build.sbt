lazy val commonSettings = Seq(
  organization := "com.stlabs",
  scalaVersion := "2.11.8"
)

lazy val scalaKafkaClient = project.in(file("client"))
  .settings(commonSettings: _*)

lazy val kafkaTestkit = project.in(file("testkit")).dependsOn(scalaKafkaClient)
  .settings(commonSettings: _*)
    