package com.grey.algorithms

import java.sql.Date

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.lit
import org.graphframes.GraphFrame

import scala.collection.parallel.mutable.ParArray

class Degrees(spark: SparkSession) {

  def degrees(edges: Dataset[Row], vertices: Dataset[Row]): Unit = {

    /**
      * Import implicits for
      * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
      * implicit conversions, e.g., converting a RDD to a DataFrames.
      * access to the "$" notation.
      */
    import spark.implicits._

    val dates = edges.select($"start_date").distinct()
      .map(x => x.getAs[Date]("start_date")).collect()

    val paths: ParArray[DataFrame] = dates.take(16).par.map{ date =>

      val graphs: GraphFrame = GraphFrame(vertices = vertices,
        edges = edges.filter($"start_date" === date))

      var inDegrees = graphs.inDegrees
      inDegrees = inDegrees.withColumn("start_date", lit(date))
      inDegrees
    }

    paths.reduce(_ union _).show(33)


  }

}
