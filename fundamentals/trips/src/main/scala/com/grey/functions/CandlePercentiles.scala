package com.grey.functions

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.parallel.mutable.ParArray

/**
  *
  * @param spark: Spark Session
  */
class CandlePercentiles(spark: SparkSession) extends CandlePoints {


  /**
    *
    * @param records: the frame of records
    * @param partition: The frame's epoch time field
    * @param field: The field whose quantiles will be calculated
    * @return
    */
  def candlePercentiles(records: DataFrame, partition: String, field: String): Array[Candle] = {

    // This import is required for
    //    the $-notation
    //    implicit conversions like converting RDDs to DataFrames
    //    encoders for most common types, which are automatically provided by importing spark.implicits._
    import spark.implicits._

    // The distinct epoch dates/times
    val partitions = records.select(col(partition))
      .distinct().map(x => x.getAs[Long](partition)).collect()

    // The quantiles, etc.
    val tiles: ParArray[Candle] = partitions.par.map{ x =>
      val data: Dataset[Row] = records.filter(col(partition) === x)
      val quantiles = data.stat.approxQuantile(col = field, probabilities = Array(0.1, 0.25, 0.5, 0.75, 0.9), relativeError = 0)

      Candle(x, quantiles(0), quantiles(1), quantiles(2), quantiles(3), quantiles(4), data.count())
    }
    tiles.toArray

  }

}
