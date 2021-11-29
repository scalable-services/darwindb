name := "immutable-btree-cluster"

version := "0.1"

scalaVersion := "2.13.7"

val AkkaVersion = "2.6.17"
lazy val AkkaHttpVersion = "10.2.7"

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

  "com.typesafe.akka" %% "akka-serialization-jackson" % AkkaVersion
)

dependencyOverrides ++= Seq(
  "com.typesafe.akka" %% "akka-discovery" % AkkaVersion
)

// in build.sbt:
enablePlugins(AkkaGrpcPlugin)
