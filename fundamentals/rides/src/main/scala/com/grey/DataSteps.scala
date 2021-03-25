package com.grey

import java.nio.file.Paths
import java.sql.Date

import com.grey.database.TableVariables
import com.grey.environment.LocalSettings
import com.grey.source.{CaseClassOf, DataRead, DataUnload}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DataType, StructType}
import org.joda.time.DateTime

import scala.util.Try
import scala.util.control.Exception


/**
  *
  * @param spark: A SparkSession instance
  */
class DataSteps(spark: SparkSession) {

  private val localSettings = new LocalSettings()
  private val dataUnload = new DataUnload(spark = spark)
  private val dataRead = new DataRead(spark = spark)
  private val caseClassOf = CaseClassOf


  /**
    *
    * @param listOfDates : List of dates
    */
  def dataSteps(listOfDates: List[DateTime], filterDate: Date): Unit = {


    /**
      * Import implicits for
      * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
      * implicit conversions, e.g., converting a RDD to a DataFrames.
      * access to the "$" notation.
      */
    import spark.implicits._


    // Table
    val tableVariables = new TableVariables()
    val create: Try[Boolean] = new com.grey.libraries.mysql.CreateTable()
      .createTable(databaseString = "mysql.flow", tableVariables = tableVariables.tableVariables())
    if (create.isFailure) {
      sys.error(create.failed.get.getMessage)
    }


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
    listOfDates.par.foreach{ dateTime =>

      // The directory into which the data of the date in question should be deposited (directoryName) and
      // the name to assign to the data file (fileString).  Note that fileString includes the path name.
      val directoryName: String = Paths.get(localSettings.dataDirectory, dateTime.toString("yyyy")).toString
      val fileString = directoryName + localSettings.localSeparator + dateTime.toString("MM") + ".json"

      // Unload
      val unload: Try[String] = dataUnload.dataUnload(dateTime = dateTime, directoryName = directoryName, fileString = fileString)

      // Read
      val read: Try[DataFrame] = if (unload.isSuccess) {
        dataRead.dataRead(dateTime = dateTime, fileString = fileString, schema = schema)
      } else {
        sys.error(unload.failed.get.getMessage)
      }

      // Filter
      val minimal: Dataset[Row] = if (read.isSuccess){
        read.get.as(caseClassOf.caseClassOf(read.get.schema))
          .filter($"start_date" > filterDate)
      } else {
        sys.error(read.failed.get.getMessage)
      }

      // Temporary
      read.get.printSchema()
      println(read.get.count())
      println(minimal.count())

      // Write
      // new DataWrite(spark = spark).dataWrite(minimal= minimal, name = dateTime.toString("yyyyMM"))


    }


  }


}
