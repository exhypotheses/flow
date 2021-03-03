package com.grey.trips.sources

import java.nio.file.Paths

import com.grey.trips.environment.LocalSettings
import org.apache.spark.sql.functions.{to_timestamp, trim, unix_timestamp}
import org.apache.spark.sql.types.{DateType, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.joda.time.DateTime

import scala.collection.parallel.immutable.ParSeq
import scala.util.Try

class Read(spark: SparkSession) {

  private val localSettings = new LocalSettings()
  private val unload = new Unload(spark = spark)
  private val restructure = new Restructure(spark)

  private val projectTimeStamp: Column => Column = (x: Column) =>
    to_timestamp(trim(x).substr(0, new InterfaceVariables(spark).projectTimeStamp.length))


  def read(listOfDates: List[DateTime], schema: StructType): Try[Unit] = {


    import spark.implicits._


    // Per time period: The host stores the data in month files
    val instances: ParSeq[Try[Unit]] = listOfDates.par.map { dateTime =>


      // The directory into which the data of the date in question should be deposited (directoryName) and
      // the name to assign to the data file (fileString).  Note that fileString includes the path name.
      val directoryName: String = Paths.get(localSettings.dataDirectory, dateTime.toString("yyyy")).toString
      val fileString = directoryName + localSettings.localSeparator + dateTime.toString("MM") + ".json"


      // Unload the data
      val data = unload.unload(dateTime = dateTime, directoryName = directoryName, fileString = fileString)


      // Hence
      if (data.isSuccess) {

        // Read-in the records of a month
        val records: DataFrame = spark.read.schema(schema).json(fileString)


        // Cast as appropriate
        var frame = records.withColumn("starting", projectTimeStamp($"started_at"))
          .withColumn("ending", projectTimeStamp($"ended_at"))
          .withColumn("start_date", $"starting".cast(DateType))
          .drop("started_at", "ended_at")


        // Renaming
        frame = frame.withColumnRenamed("starting", "started_at")
          .withColumnRenamed("ending", "ended_at")


        // Minimal
        var minimal = frame.select($"started_at", $"start_station_id",
          $"ended_at", $"end_station_id", $"duration", $"start_date")
        minimal = minimal.withColumn("start_date_epoch", unix_timestamp($"start_date"))

        // Hence
        restructure.restructure(minimal, dateTime.toString("yyyyMM"))

      } else {
        sys.error(data.failed.get.getMessage)
      }


    }

    instances.head

  }

}
