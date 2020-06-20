package com.grey.features

import org.apache.spark.sql.functions.lower
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.parallel.immutable.ParSeq

class FeaturesPlaces(spark: SparkSession) {
  /*
    The aim herein is to determine and record distinct station names, and hence build a
    library of station names
   */

  def featuresPlaces(frame: DataFrame): Unit = {

    import spark.implicits._

    // The start & end stations have the same suffixes
    val keys = List("station_id", "station_name", "station_description",
      "station_latitude", "station_longitude")

    // Fields of interest
    val placeNamesSeq: ParSeq[DataFrame] = List("start", "end").par.map{ prefix =>

      // The prefixed names and their un-prefixed counterparts
      val fields: List[(String, String)] = keys.map(suffix => (prefix + "_" + suffix, suffix))

      // Select a set of prefixed fields.  Start applying function distinct()
      var dataset = frame.selectExpr(fields.map(x => x._1): _*).distinct()

      // Rename each field to its un-prefixed counterpart
      fields.foreach{ field =>
        dataset = dataset.withColumnRenamed(field._1, field._2)
      }

      dataset

    }

    // Reduce
    val placeNames: DataFrame = placeNamesSeq.reduce(_ union _)

    val places = placeNames.select($"station_id", lower($"station_name").as("station_name"),
        lower($"station_description").as("station_description"),
        $"station_latitude", $"station_longitude").distinct()

    places.show(33)



  }

}
