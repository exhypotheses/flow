package com.grey

import java.io.File
import java.nio.file.Paths
import java.sql.Date

import com.grey.database.{DataUpload, TableVariables}
import com.grey.environment.LocalSettings
import com.grey.pre.{DataStructure, DataWrite}
import com.grey.source.{DataRead, DataUnload}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.joda.time.DateTime

import scala.util.Try
import scala.util.control.Exception


/**
  *
  * @param spark : A SparkSession instance
  */
class DataSteps(spark: SparkSession) {

  private val localSettings = new LocalSettings()
  private val dataUnload = new DataUnload(spark = spark)
  private val dataRead = new DataRead(spark = spark)
  private val dataWrite = new DataWrite()
  private val tableVariablesInstance = new TableVariables()


  /**
    *
    * @param listOfDates : List of dates
    */
  def dataSteps(listOfDates: List[DateTime], filterDate: Date): Unit = {


    // The schema of the data in question
    val schemaProperties: Try[RDD[String]] = Exception.allCatch.withTry(
      spark.sparkContext.textFile(localSettings.resourcesDirectory + "schemaOfSource.json")
    )


    // The StructType form of the schema
    val schema: StructType = if (schemaProperties.isSuccess) {
      DataType.fromJson(schemaProperties.get.collect.mkString("")).asInstanceOf[StructType]
    } else {
      sys.error(schemaProperties.failed.get.getMessage)
    }


    // Per time period: The host stores the data as month files
    val arraysOfFileObjects = listOfDates.par.map { dateTime =>

      // The directory into which the data of the date in question should be deposited (directoryName) and
      // the name to assign to the data file (fileString).  Note that fileString includes the path name.
      val directoryName: String = Paths.get(localSettings.dataDirectory, dateTime.toString("yyyy")).toString
      val fileString = directoryName + localSettings.localSeparator + dateTime.toString("MM") + ".json"

      // Unload
      val unload: Try[String] = dataUnload.dataUnload(
        dateTime = dateTime, directoryName = directoryName, fileString = fileString)

      // Read
      val read: Try[DataFrame] = if (unload.isSuccess) {
        dataRead.dataRead(dateTime = dateTime, fileString = fileString, schema = schema)
      } else {
        sys.error(unload.failed.get.getMessage)
      }

      // Structure
      val structured: Dataset[Row] = new DataStructure(spark = spark)
        .dataStructure(data = read.get, filterDate = filterDate)

      // Write
      dataWrite.dataWrite(data = structured, name = dateTime.toString("yyyyMM"))

    }


    // Set-up data for upload
    val fileObjects: Array[File] = arraysOfFileObjects.reduce(_ union _)


    // Upload
    val upload: Array[Try[Boolean]] = fileObjects.map { file =>
      new DataUpload().dataUpload(tableVariables =
        tableVariablesInstance.tableVariables(isLocal = true, infile = file.toString))
    }


    // Finally
    if (upload.head.isSuccess){
      println("The table %s has been updated"
        .format(tableVariablesInstance.tableVariables()("tableName")))
    }


  }


}
