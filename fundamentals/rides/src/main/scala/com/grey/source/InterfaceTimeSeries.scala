package com.grey.source

import java.sql.Date

import com.grey.time.{TimeFormats, TimeSequences, TimeSeries}
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
  private val isTable: Try[DataFrame] = Exception.allCatch.withTry(
    new com.grey.libraries.mysql.UnloadData(spark = spark)
      .unloadData(queryString = "SELECT DATE_FORMAT(MAX(start_date), '%Y/%m') as start, MAX(start_date) as filter from rides",
        databaseString = "mysql.flow")
  )


  /**
    *
    * @param interfaceVariables: A class of the source's key variables
    * @return
    */
  def interfaceTimeSeries(interfaceVariables: InterfaceVariables): (List[DateTime], Date) = {


    // Lower Boundary
    val (start, filter): (String, String) = if (isTable.isSuccess) {
      (isTable.get.head().getAs[String]("start"),
        isTable.get.head().getAs[String]("filter"))
    } else {
      ("", "")
    }

    val (startDate, filterString): (String, String) = if (start.isEmpty) {
      (interfaceVariables.variable("times", "startDate"), interfaceVariables.variable("times", "filterString"))
    } else {
      (start, filter)
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


    // Finally
    if (listOfDates.isSuccess){
      (listOfDates.get.distinct, java.sql.Date.valueOf(filterString))
    } else {
      sys.error(listOfDates.failed.get.getMessage)
    }


  }


}

/**
  * import java.sql.Date
  * import org.joda.time.format.DateTimeFormatter
  * import org.joda.time.DateTime
  *
  * val dateTimeFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyy-MM-dd")
  * val filterDateTime: DateTime = new TimeFormats("yyy-MM-dd").timeFormats(filterString)
  *
  * val filterDate: Date = java.sql.Date.valueOf(
  *   filterDateTime.toString(dateTimeFormatter)
  * )
  */
