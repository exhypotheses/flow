package com.grey


import java.sql.Date

import com.grey.algorithms.{EdgesData, VerticesData}
import com.grey.environment.LocalSettings
import com.grey.libraries.postgresql.UnloadData
import com.grey.types.CaseClassOf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.util.Try

class DataSteps(spark: SparkSession) {

  private val unloadData = new UnloadData(spark = spark)
  private val localSettings = new LocalSettings()
  private val caseClassOf = CaseClassOf

  private val edgesData = new EdgesData(spark = spark)
  private val verticesData = new VerticesData(spark = spark)


  def dataSteps(): Unit = {

    /**
      * Import implicits for
      * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
      * implicit conversions, e.g., converting a RDD to a DataFrames.
      * access to the "$" notation.
      */
    import spark.implicits._


    // Distinct riding dates
    val datesFrame: Try[DataFrame] = unloadData.unloadData(queryString = "SELECT distinct start_date FROM rides",
      databaseString = localSettings.databaseString)
    val dates: Array[Date] = datesFrame.get.select($"start_date")
      .map(x => x.getAs[Date]("start_date")).collect()


    // Vertices
    val verticesString = "SELECT * FROM stations"
    val vertices: Dataset[Row] = verticesData.verticesData(verticesString = verticesString)
      .persist(StorageLevel.MEMORY_ONLY)


    // Edges
    val edgesString = (value: String) => "SELECT start_station_id AS src, end_station_id AS dst, " +
      s"duration, start_date FROM rides WHERE start_date = $value AND end_station_id IS NOT NULL"


    dates.par.foreach{date =>

      



    }

  }



}
