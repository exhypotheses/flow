package com.grey.trips

import com.grey.trips.environment.LocalSettings
import com.grey.trips.functions.Quantiles
import com.grey.trips.hive.{HiveBaseProperties, HiveBaseSettings}
import com.grey.trips.sources.{CaseClassOf, Read}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime

import scala.util.Try
import scala.util.control.Exception


class DataSteps(spark: SparkSession) {

  private val localSettings = new LocalSettings()
  private val hiveBaseProperties = new HiveBaseProperties().hiveBaseProperties
  private val hiveBaseSettings = new HiveBaseSettings(spark)
  private val read = new Read(spark)

  def dataSteps(listOfDates: List[DateTime]): Unit = {

    import spark.implicits._

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

    // Setting-up
    val data: Dataset[Row] = if (features.isSuccess) {
      spark.sql("use flow")
      val data = spark.sql("select * from trips")
      val caseClassOf = CaseClassOf.caseClassOf(schema = data.schema)
      data.as(caseClassOf).select($"start_date_epoch", $"duration")
    } else {
      sys.error(features.failed.get.getMessage)
    }
    data.persist(StorageLevel.MEMORY_ONLY)


    // Spreads
    // Determine each day's riding time distributions; quantiles
    new Quantiles(spark = spark).quantiles(partitioningField = "start_date_epoch",
      calculationField = "duration", fileName = "durationCandles")


    // Daily Graph Networks & Metrics
    // For: in-flows & out-flows, busy periods, demand forecasts, data integration, as-a-bird-flies analysis


  }

}
