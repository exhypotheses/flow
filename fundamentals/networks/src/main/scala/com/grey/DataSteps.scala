package com.grey


import com.grey.algorithms.{EdgesData, VerticesData}

import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import org.graphframes.GraphFrame


/**
  *
  * @param spark: An instance of SparkSession
  */
class DataSteps(spark: SparkSession) {

  private val edgesData = new EdgesData(spark = spark)
  private val verticesData = new VerticesData(spark = spark)

  def dataSteps(): Unit = {

    /**
      * Import implicits for
      * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
      * implicit conversions, e.g., converting a RDD to a DataFrames.
      * access to the "$" notation.
      */
    import spark.implicits._


    // Vertices
    val verticesString = "SELECT * FROM stations"
    val vertices: Dataset[Row] = verticesData.verticesData(verticesString = verticesString)
      .withColumnRenamed(existingName = "station_id", newName = "id")
      .persist(StorageLevel.MEMORY_ONLY)


    // Edges
    val edgesString = "SELECT start_station_id AS src, end_station_id AS dst, duration, start_date " +
      "FROM rides WHERE end_station_id IS NOT NULL"
    val edges = edgesData.edgesData(edgesString = edgesString).persist(StorageLevel.MEMORY_ONLY)


    // Graphs
    val graphs = GraphFrame(vertices = vertices, edges = edges)


    // Hence
    val routes = graphs.edges.groupBy($"start_date", $"src", $"dst").count()
      .orderBy(desc("count"))
    routes.show(33)

    graphs.inDegrees.orderBy(desc("inDegree")).show(33)


  }


}
