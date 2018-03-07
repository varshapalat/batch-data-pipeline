package com.thoughtworks.ca.de.common.utils

import java.io.{BufferedReader, InputStreamReader}
import java.util.zip.ZipInputStream

import org.apache.spark.SparkContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD

object InputImplicits {

  implicit class ZipSparkContext(val sc: SparkContext) extends AnyVal {

    def readFile(path: String,
                 minPartitions: Int = sc.defaultMinPartitions): RDD[String] = {

      if (path.endsWith(".zip")) {
        sc.binaryFiles(path, minPartitions)
          .flatMap { case (name: String, content: PortableDataStream) =>
            val zis = new ZipInputStream(content.open)
            Stream.continually(zis.getNextEntry)
              .takeWhile {
                case null => zis.close(); false
                case _ => true
              }
              .flatMap { _ =>
                val br = new BufferedReader(new InputStreamReader(zis))
                Stream.continually(br.readLine()).takeWhile(_ != null)
              }
          }
      } else {
        sc.textFile(path, minPartitions)
      }
    }
  }

}
