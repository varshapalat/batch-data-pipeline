package com.thoughtworks.ca.de.common.utils

import java.util.Map.Entry
import scala.collection.JavaConverters._


import com.typesafe.config.{Config, ConfigObject, ConfigValue}
object ConfigUtils {
  def getObjectMap(conf: Config,path:String): Map[String,String] ={
    val confObjectList : Iterable[ConfigObject] = conf.getObjectList("ingest.sources").asScala
    (for {
      item: ConfigObject <- confObjectList
      entry: Entry[String, ConfigValue] <- item.entrySet().asScala
      key = entry.getKey
      value = entry.getValue.unwrapped().toString
    } yield (key, value)).toMap
  }
}
