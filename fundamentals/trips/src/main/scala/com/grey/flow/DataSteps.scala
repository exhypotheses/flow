package com.grey.flow

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}
import org.joda.time.DateTime

import scala.util.Try
import scala.util.control.Exception

class DataSteps(spark: SparkSession) {

  private val localSettings = new LocalSettings()
  private val interfaceVariables = new InterfaceVariables()


  def dataSteps(): Unit = {

    // The schema of the data in question
    val schemaProperties: Try[RDD[String]] = Exception.allCatch.withTry(
      spark.sparkContext.textFile(localSettings.resourcesDirectory + "schemaOfSource.json")
    )

    val schema: StructType = if (schemaProperties.isSuccess){
      DataType.fromJson(schemaProperties.get.collect.mkString("")).asInstanceOf[StructType]
    } else {
      sys.error(schemaProperties.failed.get.getMessage)
    }

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
      timeSeries.timeSeries(from, until, interfaceVariables.step, interfaceVariables.stepType )
    } else {
      sys.error("The start date must precede the end date")
    }


    val isExistURL = new IsExistURL()
    listOfDates.par.foreach{dateTime =>

      val url: String = interfaceVariables.api.format(dateTime.toString(interfaceVariables.dateTimePattern))
      val isURL: Boolean = isExistURL.isExistURL(url)

      println(url)
      println("Exists: " + isURL)
      println(dateTime.getYear)
      println(dateTime.getMonthOfYear)



    }

















  }

}
