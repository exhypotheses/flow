package com.grey.time

import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import scala.util.Try
import scala.util.control.Exception

/**
  *
  * @param dateTimePattern: Expected date and/or time pattern
  */
class TimeFormats(dateTimePattern: String) {

  /**
    *
    * @param dateString: A date and/or time string that will be verified
    * @return
    */
  def timeFormats(dateString: String): DateTime = {

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
