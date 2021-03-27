package com.grey.pre

import java.sql.Date

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class DataStructure(spark: SparkSession) {

  private val caseClassOf = CaseClassOf

  def dataStructure(read: DataFrame, filterDate: Date): Dataset[Row] = {

    /**
      * Import implicits for
      * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
      * implicit conversions, e.g., converting a RDD to a DataFrames.
      * access to the "$" notation.
      */
    import spark.implicits._


    // Filter
    val minimal: Dataset[Row] =
      read.as(caseClassOf.caseClassOf(read.schema))
        .filter($"start_date" > filterDate)

    minimal
  }

}
