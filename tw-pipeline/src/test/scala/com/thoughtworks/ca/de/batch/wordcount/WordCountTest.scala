package com.thoughtworks.ca.de.batch.wordcount

import java.nio.file.{Files, StandardOpenOption}

import com.thoughtworks.ca.de.DefaultFeatureSpecWithSpark
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter._


class WordCountTest extends DefaultFeatureSpecWithSpark {
  feature("Word count application") {
    scenario("Acceptance test for basic use") {
      Given("A simple input file, a Spark context, and a known output file")

      val rootDirectory = Files.createTempDirectory(this.getClass.getName)

      val inputCsv = Files.createFile(rootDirectory.resolve("input.txt"))
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

      WordCount.run(spark,
                    inputCsv.toUri.toString,
                    outputDirectory.toUri.toString)

      Then("It outputs files containing the expected data")

      val files = FileUtils
        .listFiles(outputDirectory.toFile,
                   new AndFileFilter(EmptyFileFilter.NOT_EMPTY,
                                     new SuffixFileFilter(".csv")),
                   TrueFileFilter.TRUE)
        .asScala

      val allLines = files
        .foldRight(Set[String]())((file, lineSet) =>
          lineSet ++ FileUtils.readLines(file).asScala)
        .map(_.trim)
      val expectedLines = Set("worst,1",
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

    ignore("Acceptance test for advanced use") {
      Given("A simple input file, a Spark context, and a known output file")

      val rootDirectory = Files.createTempDirectory(this.getClass.getName)

      val inputFile = Files.createFile(rootDirectory.resolve("input.txt"))
      val outputDirectory = rootDirectory.resolve("output")
      import scala.collection.JavaConverters._

      val lines = List(
        "Most of the big shore places were closed now and there were hardly any lights except the shadowy, moving glow of a ferryboat across the Sound. And as the moon rose higher the inessential houses began to melt away until gradually I became aware of the old island here that flowered once for Dutch sailors' eyes--a fresh, green breast of the new world. Its vanished trees, the trees that had made way for Gatsby's house, had once pandered in whispers to the last and greatest of all human dreams; for a transitory enchanted moment man must have held his breath in the presence of this continent, compelled into an aesthetic contemplation he neither understood nor desired, face to face for the last time in history with something commensurate to his capacity for wonder.",
        "And as I sat there, brooding on the old unknown world, I thought of Gatsby's wonder when he first picked out the green light at the end of Daisy's dock. He had come a long way to this blue lawn and his dream must have seemed so close that he could hardly fail to grasp it. He did not know that it was already behind him, somewhere back in that vast obscurity beyond the city, where the dark fields of the republic rolled on under the night.",
        "Gatsby believed in the green light, the orgastic future that year by year recedes before us. It eluded us then, but that's no matter--tomorrow we will run faster, stretch out our arms farther.... And one fine morning----",
        "So we beat on, boats against the current, borne back ceaselessly into the past.      "
      )
      Files.write(inputFile, lines.asJava, StandardOpenOption.CREATE)

      When("I trigger the application")

      WordCount.run(spark,
                    inputFile.toUri.toString,
                    outputDirectory.toUri.toString)

      Then("It outputs files containing the parsed, ordered, expected data")

      val files = FileUtils
        .listFiles(outputDirectory.toFile,
                   new AndFileFilter(EmptyFileFilter.NOT_EMPTY,
                                     new SuffixFileFilter(".csv")),
                   TrueFileFilter.TRUE)
        .asScala
        .toList
        .sorted

      val allLines = files
        .foldRight(List[String]())((file, lineSet) =>
          lineSet ++ FileUtils.readLines(file).asScala)
        .map(_.trim)

      val expectedLines = List(
        "a,4",
        "across,1",
        "aesthetic,1",
        "against,1",
        "all,1",
        "already,1",
        "an,1",
        "and,6",
        "any,1",
        "arms,1",
        "as,2",
        "at,1",
        "aware,1",
        "away,1",
        "back,2",
        "beat,1",
        "became,1",
        "before,1",
        "began,1",
        "behind,1",
        "believed,1",
        "beyond,1",
        "big,1",
        "blue,1",
        "boats,1",
        "borne,1",
        "breast,1",
        "breath,1",
        "brooding,1",
        "but,1",
        "by,1",
        "capacity,1",
        "ceaselessly,1",
        "city,1",
        "close,1",
        "closed,1",
        "come,1",
        "commensurate,1",
        "compelled,1",
        "contemplation,1",
        "continent,1",
        "could,1",
        "current,1",
        "daisy's,1",
        "dark,1",
        "desired,1",
        "did,1",
        "dock,1",
        "dream,1",
        "dreams,1",
        "dutch,1",
        "eluded,1",
        "enchanted,1",
        "end,1",
        "except,1",
        "eyes,1",
        "face,2",
        "fail,1",
        "farther,1",
        "faster,1",
        "ferryboat,1",
        "fields,1",
        "fine,1",
        "first,1",
        "flowered,1",
        "for,5",
        "fresh,1",
        "future,1",
        "gatsby's,2",
        "gatsby,1",
        "glow,1",
        "gradually,1",
        "grasp,1",
        "greatest,1",
        "green,3",
        "had,3",
        "hardly,2",
        "have,2",
        "he,5",
        "held,1",
        "here,1",
        "higher,1",
        "him,1",
        "his,3",
        "history,1",
        "house,1",
        "houses,1",
        "human,1",
        "i,3",
        "in,5",
        "inessential,1",
        "into,2",
        "island,1",
        "it,3",
        "its,1",
        "know,1",
        "last,2",
        "lawn,1",
        "light,2",
        "lights,1",
        "long,1",
        "made,1",
        "man,1",
        "matter,1",
        "melt,1",
        "moment,1",
        "moon,1",
        "morning,1",
        "most,1",
        "moving,1",
        "must,2",
        "neither,1",
        "new,1",
        "night,1",
        "no,1",
        "nor,1",
        "not,1",
        "now,1",
        "obscurity,1",
        "of,9",
        "old,2",
        "on,3",
        "once,2",
        "one,1",
        "orgastic,1",
        "our,1",
        "out,2",
        "pandered,1",
        "past,1",
        "picked,1",
        "places,1",
        "presence,1",
        "recedes,1",
        "republic,1",
        "rolled,1",
        "rose,1",
        "run,1",
        "sailors,1",
        "sat,1",
        "seemed,1",
        "shadowy,1",
        "shore,1",
        "so,2",
        "something,1",
        "somewhere,1",
        "sound,1",
        "stretch,1",
        "that's,1",
        "that,6",
        "the,22",
        "then,1",
        "there,2",
        "this,2",
        "thought,1",
        "time,1",
        "to,6",
        "tomorrow,1",
        "transitory,1",
        "trees,2",
        "under,1",
        "understood,1",
        "unknown,1",
        "until,1",
        "us,2",
        "vanished,1",
        "vast,1",
        "was,1",
        "way,2",
        "we,2",
        "were,2",
        "when,1",
        "where,1",
        "whispers,1",
        "will,1",
        "with,1",
        "wonder,2",
        "world,2",
        "year,2"
      )

      allLines should be(expectedLines)
      FileUtils.deleteDirectory(rootDirectory.toFile)
    }
  }
}
