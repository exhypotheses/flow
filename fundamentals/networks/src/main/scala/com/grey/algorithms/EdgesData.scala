package com.grey.algorithms

import com.grey.environment.LocalSettings
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import com.grey.libraries.postgresql.UnloadData
import com.grey.types.CaseClassOf

import scala.util.Try

class EdgesData(spark: SparkSession) {

  private val localSettings = new LocalSettings
  private val unloadData = new UnloadData(spark = spark)
  private val caseClassOf = CaseClassOf

  def edgesData(edgesString: String): Dataset[Row] = {


    // Get Edges
    val edgesFrame: Try[DataFrame] = unloadData.unloadData(queryString = edgesString,
      databaseString = localSettings.databaseString, numberOfPartitions = 4)


    // Dataset[Row]
    val edges: Dataset[Row] =if (edgesFrame.isSuccess){
      edgesFrame.get.as(caseClassOf.caseClassOf(edgesFrame.get.schema))
    } else {
      sys.error(edgesFrame.failed.get.getMessage)
    }


    // Hence
    edges

  }

}
