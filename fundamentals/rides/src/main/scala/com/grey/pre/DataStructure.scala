package com.grey.pre

import java.sql.Date

import com.grey.environment.LocalSettings
import com.grey.libraries.postgresql.UnloadData
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


/**
  *
  * @param spark: An instance of SparkSession
  */
class DataStructure(spark: SparkSession) {

  private val caseClassOf = CaseClassOf
  private val fieldsOfInterest = List("started_at", "start_station_id",
    "ended_at", "end_station_id", "duration", "start_date", "start_date_epoch")

  private val localSettings = new LocalSettings()
  private val unloadData = new UnloadData(spark = spark)

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


    // Focus on the fields of interest, switch to a Dataset Table, filter, ascertain distinct records
    val filtered: Dataset[Row] = data.selectExpr(fieldsOfInterest: _*).as(caseClassOf.caseClassOf(data.schema))
      .filter($"start_date" > filterDate).distinct()


    // Minimal: Eliminate potential duplicates
    val minimal: Dataset[Row] = filtered.except(
      unloadData.unloadData(queryString = s"SELECT $fields FROM rides",
        databaseString = localSettings.databaseString, numberOfPartitions = 4)
    )


    // Hence
    minimal


  }

}
