package com.grey.trips.hive

import com.grey.libraries.{HiveBase, HiveBaseCaseClass}
import com.grey.trips.environment.LocalSettings
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try
import scala.util.control.Exception


class HiveBaseSettings(spark: SparkSession) {


  private val localSettings = new LocalSettings()


  // The fields of the target table
  val fieldsProperties: Try[RDD[String]] = Exception.allCatch.withTry(
    spark.sparkContext
      .textFile(localSettings.resourcesDirectory + "fieldsProperties.json")
  )


  // Converting to StructType form
  val fieldsStructType: StructType = if (fieldsProperties.isSuccess) {DataType
    .fromJson(fieldsProperties.get.collect.mkString("")).asInstanceOf[StructType]
  } else {
    sys.error(fieldsProperties.failed.get.getMessage)
  }


  // String of fields for table creation
  // Switch to stringOfFields = fieldsStructType.toDDL
  // val stringOfFields: String = new HiveTableFields().hiveTableFields(fieldsStructType)
  val stringOfFields: String = fieldsStructType.toDDL


  // The array of the field names only
  val fieldsOfInterest: Array[String] = fieldsStructType.fieldNames


  // Hence
  def hiveBaseSettings(hiveBaseProperties: HiveBaseCaseClass#HBCC): Try[DataFrame] = {

    val base: Try[DataFrame] = new HiveBase(spark).hiveBase(hiveBaseProperties, stringOfFields)
    base

  }

}
