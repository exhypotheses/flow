package com.grey.trips.time

import org.joda.time.{DateTime, Period}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

class TimeSeries {

  val dateTimeNow: DateTime = DateTime.now
  val dateTimeFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy/MM/dd")
  val endingString: String = dateTimeFormatter.print(dateTimeNow.minus(Period.days(1)))
  val endingDateTime: DateTime = dateTimeFormatter.parseDateTime(endingString)

  def timeSeries(from: DateTime, until: DateTime, step: Int, stepType: String): List[DateTime] = {

    val timeSeriesIterator: Iterator[DateTime]  = stepType match {
      case "days" =>
        Iterator.iterate(from)( x => x.plusDays(step)).takeWhile(x => !x.isAfter(until) )
      case "months" =>
        Iterator.iterate(from)( x => x.plusMonths(step)).takeWhile(x => !x.isAfter(until) )
      case _ =>
        sys.error("Unknown Step Type")
    }

    timeSeriesIterator.toList

  }

}
