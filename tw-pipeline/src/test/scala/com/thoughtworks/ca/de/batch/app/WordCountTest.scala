package com.thoughtworks.ca.de.batch.app

import java.io.File
import java.nio.file.{Files, StandardOpenOption}
import java.util

import com.thoughtworks.ca.de.DefaultFeatureSpecWithSpark
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter._
import org.scalatest.BeforeAndAfter


class WordCountTest extends DefaultFeatureSpecWithSpark {
  feature("Word count application") {
    scenario("Normal use") {
      Given("A simple input file, a Spark context, and a known output file")

      val rootDirectory = Files.createTempDirectory(this.getClass.getName)

      val inputCsv = Files.createFile(rootDirectory.resolve("input.csv"))
      val outputDirectory = rootDirectory.resolve("output")
      import scala.collection.JavaConverters._
      val lines = List(
        "It was the best of times,",
        "it was the worst of times,",
        "it was the age of wisdom,",
        "it was the age of foolishness,"
      )
      Files.write(inputCsv, lines.asJava, StandardOpenOption.CREATE)
      When("I trigger the application")

      WordCount.run(spark, inputCsv.toUri.toString, outputDirectory.toUri.toString)

      Then("It outputs files containing the expected data")

      val files = FileUtils
        .listFiles(outputDirectory.toFile, new AndFileFilter(EmptyFileFilter.NOT_EMPTY, new SuffixFileFilter(".csv")), TrueFileFilter.TRUE)
        .asScala

      val allLines = files.foldRight(Set[String]())((file, lineSet) => lineSet ++ FileUtils.readLines(file).asScala).map(_.trim)
      val expectedLines = Set(
        "worst,1",
        "\"times,\",2",
        "was,4",
        "age,2",
        "it,4",
        "\"foolishness,\",1",
        "of,4",
        "\"wisdom,\",1",
        "best,1",
        "the,4")
      allLines should contain theSameElementsAs expectedLines
      FileUtils.deleteDirectory(rootDirectory.toFile)
    }
  }
}
