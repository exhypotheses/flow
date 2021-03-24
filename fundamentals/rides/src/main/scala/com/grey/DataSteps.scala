package com.grey

import java.nio.file.Paths

import com.grey.environment.LocalSettings
import com.grey.source.{DataRead, DataUnload}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataType, StructType}
import org.joda.time.DateTime

import scala.collection.parallel.immutable.ParSeq
import scala.util.Try
import scala.util.control.Exception

class DataSteps(spark: SparkSession) {

  private val localSettings = new LocalSettings()
  private val dataUnload = new DataUnload(spark = spark)
  private val dataRead = new DataRead(spark = spark)

  /**
    *
    * @param listOfDates : List of dates
    */
  def dataSteps(listOfDates: List[DateTime]): Unit = {

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
      val data: Try[DataFrame] = if (unload.isSuccess) {
        dataRead.dataRead(dateTime = dateTime, fileString = fileString, schema = schema)
      } else {
        sys.error(unload.failed.get.getMessage)
      }

      // Write
      // dataRestructure.dataRestructure(minimal, dateTime.toString("yyyyMM"), dateFilter)



    }





  }

}
