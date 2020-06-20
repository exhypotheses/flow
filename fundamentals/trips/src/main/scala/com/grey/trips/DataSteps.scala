package com.grey.trips

import java.nio.file.Paths

import com.grey.features.FeaturesSteps
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataType, StructType}
import org.joda.time.DateTime

import scala.util.Try
import scala.util.control.Exception


class DataSteps(spark: SparkSession) {

  private val localSettings = new LocalSettings()
  private val interfaceVariables = new InterfaceVariables()
  private val dataUnload = new DataUnload()

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


    // Per date
    val featuresSteps = new FeaturesSteps(spark)
    listOfDates.par.foreach { dateTime =>

      println("Starting: " + dateTime.toString(interfaceVariables.dateTimePattern))

      // The directory into which the data of the data in question should be deposited, directoryName, and
      // the name to assign to the data file, fileString.  Note that fileString includes the path name.
      val directoryName: String = Paths.get(localSettings.dataDirectory, dateTime.toString("yyyy")).toString
      val fileString = directoryName + localSettings.localSeparator + dateTime.toString("MM") + ".json"

      // Unload the data
      val unload = dataUnload.dataUnload(dateTime = dateTime, directoryName = directoryName, fileString = fileString)

      // Hence
      if (unload.isSuccess) {

        val records: DataFrame = spark.read.schema(schema).json(fileString)

        featuresSteps.featuresSteps(records, dateTime.toString("yyyyMM"))

      }

    }

  }

}
