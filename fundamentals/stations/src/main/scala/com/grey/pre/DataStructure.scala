package com.grey.pre

import com.grey.environment.LocalSettings
import com.grey.libraries.postgresql.UnloadData
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.explode

import scala.util.Try
import scala.util.control.Exception


/**
  *
  * @param spark: A SparkSession instance
  */
class DataStructure(spark: SparkSession) {

  private val localSettings = new LocalSettings()
  private val fieldsOfInterest = localSettings.fieldsOfInterest
  private val unloadData = new UnloadData(spark = spark)


  def dataStructure(data: DataFrame): Try[Dataset[Row]] = {


    /**
      * Import implicits for
      * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
      * implicit conversions, e.g., converting a RDD to a DataFrames.
      * access to the "$" notation.
      */
    import spark.implicits._


    // Data structuring: decompose horizontally
    var nests: DataFrame = data.select($"data.*", $"last_updated", $"ttl")


    // ... decompose vertically, i.e., decompose array
    nests = nests.select(explode($"stations").as("station"), $"last_updated", $"ttl")


    // ... decompose horizontally, and rename
    val stations = nests.select($"station.*")
      .select($"station_id", $"capacity", $"lat".as("latitude"), $"lon".as("longitude"),
        $"name", $"address")


    // ... ascertain distinct records
    val distinct: Try[Dataset[Row]] = Exception.allCatch.withTry(
      stations.distinct().selectExpr(fieldsOfInterest: _*)
    )


    // ... identifiers
    val identifiers: Dataset[Row] = distinct.get.select($"station_id").except(
        unloadData.unloadData(queryString = s"""SELECT station_id FROM stations""",
          databaseString = localSettings.databaseString).get
      )


    // ... deduplicate
    val deduplicated: Try[Dataset[Row]] = Exception.allCatch.withTry(
      if (!identifiers.isEmpty){
        distinct.get.join(identifiers, Seq("station_id"), "inner")
      } else {
        identifiers
      }
    )


    // Hence
    if (deduplicated.isSuccess) {
      deduplicated
    } else {
      sys.error(deduplicated.failed.get.getMessage)
    }


  }


}
