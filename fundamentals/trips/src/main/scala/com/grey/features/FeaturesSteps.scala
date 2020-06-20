package com.grey.features

import com.grey.trips.InterfaceVariables
import org.apache.spark.sql.functions.{to_timestamp, trim}
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql._


class FeaturesSteps(spark: SparkSession) {

  private val interfaceVariables = new InterfaceVariables()
  private val projectTimeStamp = interfaceVariables.projectTimeStamp

  private val featuresPlaces = new FeaturesPlaces(spark)
  private val percentilesTime = new PercentilesTime(spark)

  private val timeStampFormat: Column => Column = (x: Column) =>
    to_timestamp(trim(x).substr(0, projectTimeStamp.length))

  def featuresSteps(records: DataFrame, name: String): Unit = {

    import spark.implicits._

    var frame = records.withColumn("starting", timeStampFormat($"started_at") )
        .withColumn("ending", timeStampFormat($"ended_at"))
        .withColumn("start_date", $"starting".cast(DateType))
        .drop("started_at", "ended_at")

    frame = frame.withColumnRenamed("starting", "started_at")
      .withColumnRenamed("ending", "ended_at")

    // Places
    featuresPlaces.featuresPlaces(frame)

    // Minimal
    val minimal = frame.select($"started_at", $"start_station_id",
      $"ended_at", $"end_station_id", $"duration", $"start_date" )

    // Partition by day
    // val dailyRecords: Dataset[Row] = minimal.repartition($"start_date")

    // Percentiles
    // percentilesTime.percentilesTime(..., fields = List("start_date"), partitionFields = List($"start_date") )


    /*
    minimal.coalesce(1).write
      .option("header", "true")
      .option("quoteAll", "true")
      .option("dateFormat", "yyyy-MM-dd")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS")
      .option("encoding", "UTF-8")
      .csv(localSettings.warehouseDirectory + localSettings.localSeparator + name)
    */


  }

}
