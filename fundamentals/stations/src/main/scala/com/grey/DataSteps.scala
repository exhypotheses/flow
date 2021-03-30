package com.grey

import java.io.File

import com.grey.database.{DataUpload, TableVariables}
import com.grey.environment.LocalSettings
import com.grey.libraries.mysql.CreateTable
import com.grey.pre.{DataStructure, DataWrite}
import com.grey.source.{DataRead, DataUnload}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

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
    val unload: Try[String] = if (create.isSuccess){
      new DataUnload().dataUnload(urlString = urlString, unloadString = unloadString)
    } else {
      sys.error(create.failed.get.getMessage)
    }

    // Read
    val read: Try[DataFrame] = if (unload.isSuccess){
      new DataRead(spark = spark).dataRead(unloadString = unloadString)
    } else {
      sys.error(unload.failed.get.getMessage)
    }

    // Structure
    val structure: Try[Dataset[Row]] = if (read.isSuccess){
      new DataStructure(spark = spark).dataStructure(data = read.get)
    } else {
      sys.error(read.failed.get.getMessage)
    }

    // Write
    val fileObjects: Array[File] = new DataWrite().dataWrite(data = structure.get)

    // Upload
    val upload: Array[Try[Boolean]] = fileObjects.map{ file =>
      new DataUpload().dataUpload(tableVariables =
        tableVariablesInstance.tableVariables(isLocal = true, infile = file.toString))
    }

    // Hence
    if (upload.head.isSuccess) {
      println("The table %s has been updated"
        .format(tableVariablesInstance.tableVariables()("tableName")))
    }

  }

}
