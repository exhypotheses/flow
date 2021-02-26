package com.grey.trips.specific

import com.grey.trips.time.{TimeConstraints, TimeFormat, TimeSeries}
import org.joda.time.DateTime

import scala.util.Try
import scala.util.control.Exception

class DataTimes {

  def dataTimes(interfaceVariables: InterfaceVariables): List[DateTime] = {

    // The start/from & end/until dates of the data of interest
    val timeFormat = new TimeFormat(interfaceVariables.dateTimePattern)
    val from: DateTime = timeFormat.timeFormat(interfaceVariables.startDate)
    val until: DateTime = timeFormat.timeFormat(interfaceVariables.endDate)

    // Is from prior to until?
    val timeConstraints = new TimeConstraints()
    timeConstraints.sequentialTimes(from = from, until = until)

    // List of dates
    val timeSeries = new TimeSeries()
    val listOfDates: Try[List[DateTime]] = Exception.allCatch.withTry(
      timeSeries.timeSeries(from, until, interfaceVariables.step, interfaceVariables.stepType)
    )

    if (listOfDates.isSuccess){
      listOfDates.get.distinct
    } else {
      sys.error(listOfDates.failed.get.getMessage)
    }


  }

}
