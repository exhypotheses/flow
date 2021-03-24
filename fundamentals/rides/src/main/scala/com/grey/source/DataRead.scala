package com.grey.source

import com.grey.environment.LocalSettings
import org.apache.spark.sql.functions.{to_timestamp, trim, unix_timestamp}
import org.apache.spark.sql.types.{DateType, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.joda.time.DateTime

import scala.util.Try
import scala.util.control.Exception


/**
  *
  * @param spark: A SparkSession instance
  */
class DataRead(spark: SparkSession) {

  private val localSettings = new LocalSettings()

  private val projectTimeStamp: Column => Column = (x: Column) =>
    to_timestamp(trim(x).substr(0, localSettings.projectTimeStamp.length))


  /**
    *
    * @param dateTime: The date/time in focus
    * @param fileString: The file to be read (the string includes the path string)
    * @param schema: The schema of the data file to be read
    * @return
    */
  def dataRead(dateTime: DateTime, fileString: String, schema: StructType): Try[DataFrame] = {


    /**
      * Import implicits for
      * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
      * implicit conversions, e.g., converting a RDD to a DataFrames.
      * access to the "$" notation.
      */
    import spark.implicits._


    // Read-in the records of a month
    val records: Try[DataFrame] = Exception.allCatch.withTry(
      spark.read.schema(schema).json(fileString)
    )
    if (records.isFailure) {
      sys.error(records.failed.get.getMessage)
    }


    // Cast as appropriate
    var frame = records.get.withColumn("starting", projectTimeStamp($"started_at"))
      .withColumn("ending", projectTimeStamp($"ended_at"))
      .withColumn("start_date", $"starting".cast(DateType))
      .drop("started_at", "ended_at")


    // Renaming
    frame = frame.withColumnRenamed("starting", "started_at")
      .withColumnRenamed("ending", "ended_at")


    // Relevant
    val relevant: Try[DataFrame] = Exception.allCatch.withTry(
      frame.select($"started_at", $"start_station_id",
        $"ended_at", $"end_station_id", $"duration", $"start_date")
        .withColumn("start_date_epoch", unix_timestamp($"start_date"))
    )

    if (relevant.isSuccess) {
      relevant
    } else {
      sys.error(relevant.failed.get.getMessage)
    }
    

  }

}
