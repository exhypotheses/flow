package com.grey.trips.functions

import java.sql.Date

import com.grey.trips.hive.HiveBaseSettings
import com.grey.trips.specific.InterfaceVariables
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.util.Try
import scala.util.control.Exception

class DataWrite(spark: SparkSession) {

  private val interfaceVariables = new InterfaceVariables(spark)
  private val fieldsOfInterest: Array[String] = new HiveBaseSettings(spark).fieldsOfInterest

  def dataWrite(daily: Dataset[Row], date: Date, src: String): Try[Unit] = {

    println(daily.show(5))

    // Beware of time stamps, e.g., "yyyy-MM-dd HH:mm:ss.SSSXXXZ"
    val write: Try[Unit] = Exception.allCatch.withTry(
      daily.selectExpr(fieldsOfInterest: _*).write
        .option("header", "false")
        .option("encoding", "UTF-8")
        .option("timestampFormat", interfaceVariables.projectTimeStamp)
        .csv(src)
    )

    // State
    if (write.isSuccess) {
      write
    } else {
      sys.error(write.failed.get.getMessage)
    }

  }


}
