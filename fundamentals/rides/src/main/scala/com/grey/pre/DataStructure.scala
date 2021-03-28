package com.grey.pre

import java.sql.Date

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


/**
  *
  * @param spark: An instance of SparkSession
  */
class DataStructure(spark: SparkSession) {

  private val caseClassOf = CaseClassOf
  private val fieldsOfInterest = List("started_at", "start_station_id",
    "ended_at", "end_station_id", "duration", "start_date", "start_date_epoch")


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


    // Filter
    val minimal: Dataset[Row] =
      data.as(caseClassOf.caseClassOf(data.schema))
        .filter($"start_date" > filterDate)
    minimal.selectExpr(fieldsOfInterest: _*)

  }

}
