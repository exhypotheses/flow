package com.grey.database

import java.sql.Date

import com.grey.environment.LocalSettings
import com.grey.libraries.postgresql.UnloadData
import com.grey.source.InterfaceVariables
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try


/**
  *
  * @param spark: An instance of SparkSession
  */
class DataStartUp(spark: SparkSession) {

  private val unloadData = new UnloadData(spark = spark)
  private val localSettings = new LocalSettings()


  /**
    *
    * @return The number of records in 'rides', at present
    */
  def shape(): Long = {

    // Number of records
    unloadData.unloadData(queryString = s"SELECT COUNT(*) AS n FROM rides",
      databaseString = localSettings.databaseString).get.head.getAs[Long]("n")
  }


  /**
    *
    * @param interfaceVariables: Interface variables
    *
    * @return The starting dates strings
    */
  def dates(interfaceVariables: InterfaceVariables): (String, String) = {

    // Lower Boundary
    if (shape() > 0) {

      val isTable: Try[DataFrame] = unloadData.unloadData(
        queryString = "SELECT to_char(MAX(start_date), 'YYYY/MM') as start, MAX(start_date) as filter from rides",
        databaseString = localSettings.databaseString)

      (isTable.get.head().getAs[String]("start"),
        isTable.get.head().getAs[Date]("filter").toString)

    } else {
      (interfaceVariables.variable("times", "startDate"),
        interfaceVariables.variable("times", "filterString"))
    }

  }


  /**
    *
    * @return The stations
    */
  def stations(): DataFrame = {
    unloadData.unloadData(queryString = "SELECT station_id FROM stations",
      databaseString = localSettings.databaseString).get
  }


}
