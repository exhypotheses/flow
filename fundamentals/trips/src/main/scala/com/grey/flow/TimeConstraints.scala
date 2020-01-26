package com.grey.flow

import org.joda.time.DateTime

import scala.util.control.Exception

import scala.util.Try

class TimeConstraints {

  def sequentialTimes(from: DateTime, until: DateTime): Boolean = {

    val sequential: Try[Boolean] = Exception.allCatch.withTry(
      from.isBefore(until)
    )

    if (sequential.isSuccess) {
      if (sequential.get) sequential.get else sys.error("The start date must precede the end date")
    } else {
      sys.error(sequential.failed.get.getMessage)
    }

  }

}
