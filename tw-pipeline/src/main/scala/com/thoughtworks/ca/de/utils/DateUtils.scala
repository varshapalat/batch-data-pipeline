package com.thoughtworks.ca.de.utils

import org.joda.time.DateTime
import java.text.SimpleDateFormat
import java.util.Date

object DateUtils {
  def parseISO2TWFormat(isoDate: String): String = {
    new SimpleDateFormat("yyyyMMdd").format(new DateTime(isoDate).toDate)
  }

  def date2TWFormat(): String = {
    new SimpleDateFormat("yyyyMMdd").format(new Date())
  }
}
