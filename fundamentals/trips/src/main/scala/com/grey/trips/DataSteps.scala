package com.grey.trips

import com.grey.trips.environment.LocalSettings
import com.grey.trips.hive.{HiveBaseProperties, HiveBaseSettings}
import com.grey.trips.sources.Read
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}
import org.joda.time.DateTime

import scala.util.Try
import scala.util.control.Exception


class DataSteps(spark: SparkSession) {

  private val localSettings = new LocalSettings()
  private val hiveBaseProperties = new HiveBaseProperties().hiveBaseProperties
  private val hiveBaseSettings = new HiveBaseSettings(spark)
  private val read = new Read(spark)

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

    // Features Engineering
    val features: Try[Unit] = read.read(listOfDates = listOfDates, schema = schema)

    // Inspect
    if (features.isSuccess) {
      spark.sql("use flow")
      println(spark.sql("select distinct start_date from trips").show(180))
      println(spark.sql("select distinct start_date from trips").count())
      spark.sql("select * from trips limit 5").show()
    } else {
      sys.error(features.failed.get.getMessage)
    }

    // Spreads
    // Determine each day's riding time distributions; quantiles

    // Daily Graph Networks & Metrics
    // For: in-flows & out-flows, busy periods, demand forecasts, data integration, as-a-bird-flies analysis


  }

}
