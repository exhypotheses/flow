package com.grey.trips.time

import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import scala.util.Try
import scala.util.control.Exception

class TimeFormat(dateTimePattern: String) {

  def timeFormat(dateString: String): DateTime = {

    val dateTimeFormatter: DateTimeFormatter = DateTimeFormat.forPattern(dateTimePattern)

    val isDate: Try[DateTime] = Exception.allCatch.withTry(
      dateTimeFormatter.parseDateTime(dateString)
    )

    if (isDate.isFailure) {
      sys.error(isDate.failed.get.getMessage)
    } else {
      isDate.get
    }

  }

}
