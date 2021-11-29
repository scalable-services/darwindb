organization := "services.scalable"
name := "darwindb"

version := "0.2"

scalaVersion := "2.13.7"

lazy val AkkaVersion = "2.6.17"
lazy val AkkaHttpVersion = "10.2.7"
lazy val Pulsar4sVersion = "2.7.3"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
  "com.typesafe.akka" %% "akka-actor" % AkkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % AkkaVersion,
  "org.scalatest" %% "scalatest" % "3.1.1" % Test,
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.apache.commons" % "commons-lang3" % "3.8.1",

  "com.typesafe.akka" %% "akka-cluster-sharding-typed" % AkkaVersion,

  "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
  "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
  "com.typesafe.akka" %% "akka-serialization-jackson" % AkkaVersion,
  "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8",

  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.0",

  "com.typesafe.akka" %% "akka-serialization-jackson" % AkkaVersion,

  "org.apache.pulsar" % "pulsar-client" % "2.8.1",
  "org.apache.pulsar" % "pulsar-client-admin" % "2.8.1",

  "com.sksamuel.pulsar4s" %% "pulsar4s-core" % Pulsar4sVersion,

  // for the akka-streams integration
  "com.sksamuel.pulsar4s" %% "pulsar4s-akka-streams" % Pulsar4sVersion,

  // if you want to use play-json for schemas
  "com.sksamuel.pulsar4s" %% "pulsar4s-play-json" % Pulsar4sVersion,

  "com.datastax.oss" % "java-driver-core" % "4.13.0",
)

dependencyOverrides ++= Seq(
  "org.scala-lang.modules" %% "scala-java8-compat" % "1.0.2",
  "com.typesafe.akka" %% "akka-discovery" % AkkaVersion
)

// in build.sbt:
enablePlugins(AkkaGrpcPlugin)
