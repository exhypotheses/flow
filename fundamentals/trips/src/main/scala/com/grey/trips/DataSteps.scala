package com.grey.trips

import java.nio.file.Paths

import com.grey.trips.environment.LocalSettings
import com.grey.trips.features.{FeaturesData, FeaturesInterface}
import com.grey.trips.hive.{HiveBaseProperties, HiveBaseSettings}
import com.grey.trips.specific.DataUnload
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime

import scala.collection.parallel.immutable.ParSeq
import scala.util.Try
import scala.util.control.Exception


class DataSteps(spark: SparkSession) {

  private val localSettings = new LocalSettings()

  private val hiveBaseProperties = new HiveBaseProperties().hiveBaseProperties
  private val hiveBaseSettings = new HiveBaseSettings(spark)

  private val featuresInterface = new FeaturesInterface(spark)

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


    // Ascertaining the existence of the database & data table of interest
    hiveBaseSettings.hiveBaseSettings(hiveBaseProperties)


    // F
    val features: Try[Unit] = featuresInterface.featuresInterface(listOfDates = listOfDates, schema = schema)


    // Hence
    if (features.isSuccess) {
      spark.sql("use flow")
      println(spark.sql("select distinct start_date from trips order by start_date limit 100")
        .show(100))
    } else {
      sys.error(features.failed.get.getMessage)
    }


  }

}
