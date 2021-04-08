package com.grey

import com.grey.environment.LocalSettings
import com.grey.libraries.postgresql.UnloadData
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

class DataSteps(spark: SparkSession) {

  private val unloadData = new UnloadData(spark = spark)
  private val localSettings = new LocalSettings()

  def dataSteps(): Unit = {

    val queryString = "SELECT start_station_id AS src, end_station_id AS dst, " +
      "duration, start_date, start_date_epoch FROM rides WHERE start_date >= '2021-01-01'"

    val edges: Try[DataFrame] = unloadData.unloadData(queryString = queryString,
      databaseString = localSettings.databaseString, numberOfPartitions = 4)
    edges.get.show()

    val vertices = unloadData.unloadData(queryString = "SELECT * FROM stations",
      databaseString = localSettings.databaseString)
    vertices.get.show()

  }

}
