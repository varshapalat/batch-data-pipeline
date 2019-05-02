
scalaVersion := "2.11.8"

val sparkVersion = "2.4.0"
val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5" % "test"
val sparkModuleId = (name: String) => "org.apache.spark" %% s"spark-$name" % "2.4.0"
val spark = (name: String) => sparkModuleId(name) % "provided"
val spark_test = (name: String) => sparkModuleId(name) % Test classifier "tests"

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.thoughtworks.cd.de",
      scalaVersion := "2.12.4",
      version := "0.1.0-SNAPSHOT"
    )),

    name := "tw-pipeline",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "com.typesafe" % "config" % "1.3.2",
      "org.scalatest" %% "scalatest" % "3.0.5" % "test",
      spark_test("core"),
      spark_test("sql"),
      spark_test("catalyst")
    )
  )
