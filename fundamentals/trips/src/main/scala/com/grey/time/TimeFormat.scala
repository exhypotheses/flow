package com.grey.time

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.control.Exception

class TimeFormat(dateTimePattern: String) {

  def timeFormat(dateString: String): DateTime = {

    val dateTimeFormatter = DateTimeFormat.forPattern(dateTimePattern)

    val isDate = Exception.allCatch.withTry(
      dateTimeFormatter.parseDateTime(dateString)
    )

    if (isDate.isFailure) {
      sys.error(isDate.failed.get.getMessage)
    } else {
      isDate.get
    }

  }

}
