package com.grey

import com.grey.database.TableVariables
import com.grey.environment.LocalSettings
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

    val tableVariables = new TableVariables().tableVariables()
    println( tableVariables("uploadString").format("LOCAL", unloadString, "REPLACE") )
    println( tableVariables("stringCreateTable") )



    // Unload
    val dataUnload: Try[String] = new DataUnload().dataUnload(urlString = urlString, unloadString = unloadString)


    // Transform
    if (dataUnload.isSuccess) {
      new DataTransform(spark = spark).dataTransform(unloadString = unloadString)
    } else {
      sys.error(dataUnload.failed.get.getMessage)
    }


  }

}
