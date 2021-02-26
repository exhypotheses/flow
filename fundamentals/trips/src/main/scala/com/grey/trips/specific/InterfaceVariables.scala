package com.grey.trips.specific

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.Try
import scala.util.control.Exception


/**
  * The earliest data month is "2018/12"
  * https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html
  * private val monthLimitDateTime: DateTime =
  *     DateTimeFormat.forPattern(dateTimePattern).parseDateTime(monthLimitString)
  *
  * @param spark: Instance
  */
class InterfaceVariables(spark: SparkSession) {


  // In general, w.r.t. data origin
  val step = 1
  val stepType = "months"
  val dateTimePattern = "yyyy/MM"


  // Usage: api.format(dateTimePattern)
  val api = "https://data.urbansharing.com/edinburghcyclehire.com/trips/v1/%s.json"


  // Patterns
  val projectTimeStamp = "yyyy-MM-dd HH:mm:ss.SSS"
  val sourceTimeStamp = "yyyy-MM-dd HH:mm:ss.SSSXXXZ"


  // Ending
  private val dateTimeNow: DateTime = DateTime.now
  private val monthLimitString: String = DateTimeFormat.forPattern(dateTimePattern).print(dateTimeNow)
  val endDate: String = monthLimitString // "2019/04"


  // Starting
  val isDatabase: Try[DataFrame] = Exception.allCatch.withTry(
    spark.sql("use flow")
  )

  val isTable: Try[DataFrame] = Exception.allCatch.withTry(
    spark.sql("select date_format(max(start_date), 'yyyy/MM') as maximum from trips")
  )

  val startDate: String = if (isDatabase.isSuccess && isTable.isSuccess) {
    isTable.get.head().getAs[String]("maximum")
  } else {
    "2018/12"
  }


}
