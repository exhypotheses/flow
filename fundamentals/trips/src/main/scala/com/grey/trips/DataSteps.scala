package com.grey.trips

import java.nio.file.Paths

import com.grey.trips.environment.LocalSettings
import com.grey.trips.hive.{HiveBaseProperties, HiveBaseSettings}
import com.grey.trips.source.{Read, Unload}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}
import org.joda.time.DateTime

import scala.collection.parallel.immutable.ParSeq
import scala.util.Try
import scala.util.control.Exception


/**
  *
  * @param spark: A SparkSession instance
  */
class DataSteps(spark: SparkSession) {

  private val localSettings = new LocalSettings()
  private val hiveBaseProperties = new HiveBaseProperties().hiveBaseProperties
  private val hiveBaseSettings = new HiveBaseSettings(spark)

  private val unload = new Unload(spark = spark)
  private val read = new Read(spark)


  /**
    *
    * @param listOfDates : List of dates
    */
  def dataSteps(listOfDates: List[DateTime]): Try[Unit] = {


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


    // Ascertaining the existence of the database & data table of interest
    hiveBaseSettings.hiveBaseSettings(hiveBaseProperties)


    // Per time period: The host stores the data as month files
    val instances: ParSeq[Try[Unit]] = listOfDates.par.map { dateTime =>

      // The directory into which the data of the date in question should be deposited (directoryName) and
      // the name to assign to the data file (fileString).  Note that fileString includes the path name.
      val directoryName: String = Paths.get(localSettings.dataDirectory, dateTime.toString("yyyy")).toString
      val fileString = directoryName + localSettings.localSeparator + dateTime.toString("MM") + ".json"

      // Unload the data
      val data = unload.unload(dateTime = dateTime, directoryName = directoryName, fileString = fileString)

      // Hence
      if (data.isSuccess) {
        read.read(dateTime = dateTime, fileString = fileString, schema = schema)
      } else {
        sys.error(data.failed.get.getMessage)
      }

    }
    instances.head


  }


}
