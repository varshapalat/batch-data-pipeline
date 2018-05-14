package com.thoughtworks.ca.de.common.utils

import com.thoughtworks.ca.de.common.utils.ConfigUtils
import com.typesafe.config.ConfigFactory
import org.scalatest._

class ConfigUtilsSpec extends FeatureSpec with GivenWhenThen{
  info("As a user of Config Utilities")
  info("I want to be able to parse array of key-value pairs to Scala Map")
  info("So it can be iterated over and looked up")

  feature("Convert configuration array of tupples to Map") {
    scenario("User passes configuration object and path to array of tupples") {

      Given("testMap Configuration defined in application.conf")
      val conf = ConfigFactory.load

      When("Config Utils is used")
      val resultMap = ConfigUtils.getObjectMap(conf,"testMap")
      val expectedMap=Map("key1"->"value1","key2"->"value2")

      Then("Resulting map contains correct key value pairs")
      val diffMap = (resultMap.toSet diff expectedMap.toSet).toMap
      assert(diffMap.isEmpty)
    }
  }
}
