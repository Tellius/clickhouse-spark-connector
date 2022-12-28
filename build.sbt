organization := "com.clickhouse"

name := "clickhouse-spark-connector"

version := "1.2.9"

scalaVersion := "2.12.17"

publishTo := Some("jFrog" at "https://10.2.95.5:8080/artifactory/libs-release")
//credentials += Credentials("jFrog", "10.2.95.5", "admin", "password")

scalacOptions ++= Seq("-feature", "-Ywarn-dead-code", "-Ywarn-unused", "-deprecation", "-unchecked")

libraryDependencies ++= {
  val sparkV = "3.3.1"
  
  Seq(
    "org.apache.spark" %% "spark-core" % sparkV,
    "org.apache.spark" %% "spark-sql" % sparkV,
    "com.clickhouse" % "clickhouse-jdbc" % "0.3.2-patch11",
    "org.scalatest" %% "scalatest" % "3.0.9" % Test,
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.14.1"
  )
}

run / fork := true

assembly / test := {}

assembly / assemblyMergeStrategy := {
  case n if n.startsWith("META-INF/MANIFEST.MF") => MergeStrategy.discard
  case "reference.conf"                          => MergeStrategy.concat
  case x => MergeStrategy.first
}

enablePlugins(PackPlugin)