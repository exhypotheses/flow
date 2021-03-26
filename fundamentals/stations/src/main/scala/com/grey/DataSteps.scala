package com.grey

import com.grey.database.TableVariables
import com.grey.environment.{DataDirectories, LocalSettings}
import com.grey.libraries.mysql.CreateTable
import com.grey.source.{DataTransform, DataUnload}
import org.apache.spark.sql.SparkSession

import scala.language.postfixOps
import scala.util.Try


/**
  *
  * @param spark: A SparkSession instance
  */
class DataSteps(spark: SparkSession) {

  private val localSettings = new LocalSettings()

  /**
    * Steps
    */
  def dataSteps(): Unit = {


    // The stations
    val fileString: String = "station_information.json"
    val urlString: String = s"https://gbfs.urbansharing.com/edinburghcyclehire.com/$fileString"
    val unloadString: String = localSettings.dataDirectory + fileString


    // Databases & Tables
    val tableVariablesInstance = new TableVariables()
    val createTable = new CreateTable()
    val create: Try[Boolean] = createTable.createTable(databaseString = "mysql.flow",
      tableVariables = tableVariablesInstance.tableVariables())


    // Unload
    val dataUnload: Try[String] = if (create.isSuccess){
      new DataUnload().dataUnload(urlString = urlString, unloadString = unloadString)
    } else {
      sys.error(create.failed.get.getMessage)
    }


    //  Transform: The data file string is a directory path + file name + file extension string
    //  from whence data is uploaded into a database table
    val dataFileString: Try[String] = if (dataUnload.isSuccess) {
      new DataTransform(spark = spark).dataTransform(unloadString = unloadString)
    } else {
      sys.error(dataUnload.failed.get.getMessage)
    }


    /**
      * // Upload
      * val upload: Try[Boolean] = if (dataFileString.isSuccess){
      * new DataUpload().dataUpload(
      * tableVariables = tableVariablesInstance.tableVariables(infile = dataFileString.get)
      * )
      * } else {
      *         sys.error(dataFileString.failed.get.getMessage)
      * }
      */


    /**
      * // Hence
      *
      * if (upload.isSuccess){
      *   println("The table %s has been updated"
      * .format(tableVariablesInstance.tableVariables()("tableName")))
      * }
      */


    // Preliminary
    if (dataFileString.isSuccess){
      new DataDirectories()
        .localDirectoryDelete(directoryName = localSettings.warehouseDirectory)
    } else {
      sys.error(dataFileString.failed.get.getMessage)
    }


  }


}
