name := "spark-ecommerce-project"

version := "1.0.0"

scalaVersion := "2.12.18"

val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion,
  "com.typesafe"      % "config"     % "1.4.3",
  "org.scalatest"    %% "scalatest"  % "3.2.17" % Test
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf"              => MergeStrategy.concat
  case _                             => MergeStrategy.first
}

assembly / mainClass := Some("com.ecommerce.MainApp")
