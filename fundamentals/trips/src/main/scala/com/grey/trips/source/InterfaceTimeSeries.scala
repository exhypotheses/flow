package com.grey.trips.source

import com.grey.trips.time.{TimeFormats, TimeSequences, TimeSeries}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.Try
import scala.util.control.Exception

/**
  *
  * @param spark: A SparkSession instance
  */
class InterfaceTimeSeries(spark: SparkSession) {


  // Today
  private val dateTimeNow: DateTime = DateTime.now

  // Starting
  private val isDatabase: Try[DataFrame] = Exception.allCatch.withTry(
    spark.sql("use flow")
  )

  private val isTable: Try[DataFrame] = Exception.allCatch.withTry(
    spark.sql("select date_format(max(start_date), 'yyyy/MM') as maximum from trips")
  )


  /**
    *
    * @param interfaceVariables: A class of the source's key variables
    * @return
    */
  def interfaceTimeSeries(interfaceVariables: InterfaceVariables): List[DateTime] = {


    // Lower Boundary
    val date: String = if (isDatabase.isSuccess && isTable.isSuccess) {
      isTable.get.head().getAs[String]("maximum")
    } else {
      ""
    }

    val startDate: String = if (date.isEmpty) {
      interfaceVariables.variable("times", "startDate")
    } else {
      date
    }


    // Upper Boundary
    val endDate: String = if (interfaceVariables.variable("times", "endDate").isEmpty) {
      DateTimeFormat.forPattern(interfaceVariables.dateTimePattern).print(dateTimeNow)
    } else {
      interfaceVariables.variable("times", "endDate")
    }


    // The DateTime forms of the start/from & end/until dates
    val timeFormats = new TimeFormats(interfaceVariables.dateTimePattern)
    val from: DateTime = timeFormats.timeFormats(startDate)
    val until: DateTime = timeFormats.timeFormats(endDate)


    // Is from prior to until?
    new TimeSequences().timeSequences(from = from, until = until)


    // Hence, the list of dates
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
