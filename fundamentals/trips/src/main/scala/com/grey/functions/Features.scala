package com.grey.functions

import com.grey.trips.InterfaceVariables
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{to_timestamp, trim, unix_timestamp}
import org.apache.spark.sql.types.DateType


class Features(spark: SparkSession) {

  private val interfaceVariables = new InterfaceVariables()
  private val projectTimeStamp = interfaceVariables.projectTimeStamp
  private val timeStampFormat: Column => Column = (x: Column) =>
    to_timestamp(trim(x).substr(0, projectTimeStamp.length))

  def features(records: DataFrame, name: String): DataFrame = {

    // This import is required for
    //    the $-notation
    //    implicit conversions like converting RDDs to DataFrames
    //    encoders for most common types, which are automatically provided by importing spark.implicits._
    import spark.implicits._

    // Casting
    var frame = records.withColumn("starting", timeStampFormat($"started_at"))
      .withColumn("ending", timeStampFormat($"ended_at"))
      .withColumn("start_date", $"starting".cast(DateType))
      .drop("started_at", "ended_at")

    // Renaming
    frame = frame.withColumnRenamed("starting", "started_at")
      .withColumnRenamed("ending", "ended_at")

    // Minimal
    val minimal = frame.select($"started_at", $"start_station_id",
      $"ended_at", $"end_station_id", $"duration", $"start_date")

    // Write
    /*
    minimal.coalesce(1).write
      .option("header", "true")
      .option("quoteAll", "true")
      .option("dateFormat", "yyyy-MM-dd")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS")
      .option("encoding", "UTF-8")
      .csv(localSettings.warehouseDirectory + localSettings.localSeparator + name)
    */

    // Probably
    minimal.withColumn("start_epoch", unix_timestamp($"start_date"))

  }

}
