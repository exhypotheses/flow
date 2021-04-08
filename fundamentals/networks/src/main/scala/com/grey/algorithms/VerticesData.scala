package com.grey.algorithms

import com.grey.environment.LocalSettings
import com.grey.libraries.postgresql.UnloadData
import com.grey.types.CaseClassOf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.util.Try

class VerticesData(spark: SparkSession) {

  private val localSettings = new LocalSettings()
  private val unloadData = new UnloadData(spark = spark)
  private val caseClassOf = CaseClassOf

  def verticesData(verticesString: String): Dataset[Row] = {


    // Get Vertices
    val verticesFrame: Try[DataFrame] = unloadData.unloadData(queryString = verticesString,
      databaseString = localSettings.databaseString)


    // Dataset[Row]
    val vertices: Dataset[Row] = if (verticesFrame.isSuccess) {
      verticesFrame.get.as(caseClassOf.caseClassOf(verticesFrame.get.schema))

    } else {
      sys.error(verticesFrame.failed.get.getMessage)
    }


    // Hence
    vertices

  }

}
