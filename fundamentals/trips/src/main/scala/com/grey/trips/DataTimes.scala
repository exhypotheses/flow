package com.grey.trips

import com.grey.time.{TimeConstraints, TimeFormat, TimeSeries}
import org.joda.time.DateTime

class DataTimes {

  private val interfaceVariables = new InterfaceVariables()

  def dataTimes(): List[DateTime] = {

    // The start/from & end/until dates of the data of interest
    val timeFormat = new TimeFormat(interfaceVariables.dateTimePattern)
    val from: DateTime = timeFormat.timeFormat(interfaceVariables.startDate)
    val until: DateTime = timeFormat.timeFormat(interfaceVariables.endDate)

    // Is from prior to until?
    val timeConstraints = new TimeConstraints()
    val sequentialTimes = timeConstraints.sequentialTimes(from = from, until = until)

    // List of dates
    val listOfDates: List[DateTime] = if (sequentialTimes) {
      val timeSeries = new TimeSeries()
      timeSeries.timeSeries(from, until, interfaceVariables.step, interfaceVariables.stepType)
    } else {
      sys.error("The start date must precede the end date")
    }

    listOfDates

  }

}
