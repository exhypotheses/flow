package com.grey

import com.grey.database.TableVariables
import com.grey.environment.LocalSettings
import com.grey.libraries.mysql.CreateTable
import com.grey.source.{DataTransform, DataUnload}
import org.apache.spark.sql.SparkSession

import scala.language.postfixOps
import scala.util.Try

class DataSteps(spark: SparkSession) {

  private val localSettings = new LocalSettings()

  def dataSteps(): Unit = {


    /**
      * Import implicits for
      * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
      * implicit conversions, e.g., converting a RDD to a DataFrames.
      * access to the "$" notation.
      */


    // The stations
    val fileString: String = "station_information.json"
    val urlString: String = s"https://gbfs.urbansharing.com/edinburghcyclehire.com/$fileString"
    val unloadString: String = localSettings.dataDirectory + fileString


    // Databases & Tables
    val tableVariables = new TableVariables().tableVariables(isLocal = true, infile = unloadString, duplicates = "replace")
    val createTable = new CreateTable()
    createTable.createTable(databaseName = "mysql.flow", tableVariables = tableVariables)


    // Unload
    val dataUnload: Try[String] = new DataUnload().dataUnload(urlString = urlString, unloadString = unloadString)


    // Transform
    val dataFileString: Try[String] = if (dataUnload.isSuccess) {
      new DataTransform(spark = spark).dataTransform(unloadString = unloadString)
    } else {
      sys.error(dataUnload.failed.get.getMessage)
    }
    println(dataFileString.get)


  }

}
