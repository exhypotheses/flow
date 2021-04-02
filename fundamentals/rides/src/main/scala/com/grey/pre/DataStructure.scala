package com.grey.pre

import java.sql.Date

import com.grey.database.DataStartUp
import com.grey.environment.LocalSettings
import com.grey.libraries.postgresql.UnloadData
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


/**
  *
  * @param spark: An instance of SparkSession
  */
class DataStructure(spark: SparkSession) {

  private val caseClassOf = CaseClassOf
  private val localSettings = new LocalSettings()
  private val unloadData = new UnloadData(spark = spark)
  private val dataStartUp = new DataStartUp(spark = spark)

  private val stations = dataStartUp.stations()
  private val fieldsOfInterest = localSettings.fieldsOfInterest

  /**
    *
    * @param data: The data in focus
    * @param filterDate: Ensures that the data consists of new data records, i.e., records beyond
    *                    the maximum date in the relevant database table.
    * @return
    */
  def dataStructure(data: DataFrame, filterDate: Date): Dataset[Row] = {

    /**
      * Import implicits for
      * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
      * implicit conversions, e.g., converting a RDD to a DataFrames.
      * access to the "$" notation.
      */
    import spark.implicits._


    // String of fields
    val fields: String = fieldsOfInterest.mkString(", ")


    // Plausible
    var plausible = data.join(stations, data("start_station_id") === stations("station_id"), "inner")
      .drop("station_id")
    plausible = plausible.join(stations, plausible("end_station_id") === stations("station_id"), "leftouter")
      .drop("station_id")


    // Focus on the fields of interest, switch to a Dataset Table, filter, ascertain distinct records
    val filtered: Dataset[Row] = plausible.selectExpr(fieldsOfInterest: _*).as(caseClassOf.caseClassOf(plausible.schema))
      .filter($"start_date" > filterDate).distinct()


    // Minimal: Eliminate potential duplicates
    val shape: Long = dataStartUp.shape()

    val minimal: Dataset[Row] = if (shape == 0) {
      filtered
    } else {
      filtered.except(
        unloadData.unloadData(queryString = s"SELECT $fields FROM rides",
          databaseString = localSettings.databaseString, numberOfPartitions = 4).get
      )
    }


    // Hence
    minimal


  }

}
