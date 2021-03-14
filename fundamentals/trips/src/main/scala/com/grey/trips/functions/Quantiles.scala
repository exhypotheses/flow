package com.grey.trips.functions

import com.grey.trips.source.CaseClassOf
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

class Quantiles(spark: SparkSession) extends CandleClass {

  def quantiles(partitioningField: String, calculationField: String, fileName: String): Unit = {

    // This import is required for
    //    the $-notation
    //    implicit conversions like converting RDDs to DataFrames
    //    encoders for most common types, which are automatically provided by importing spark.implicits._
    import spark.implicits._

    // Query
    val frameClause = "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"

    val query =
      s"""
         |select distinct start_date_epoch as epoch,
         |  percentile($calculationField, array(0.10, 0.25, 0.50, 0.75, 0.90))
         |    over (partition by $partitioningField $frameClause) as quantiles,
         |  count($calculationField)
         |    over (partition by $partitioningField $frameClause) as tally
         |from trips
     """.stripMargin

    // Retrieve the required set of quantile & tally calculations set-up via 'query'
    val calculations = spark.sql(query)

    // Dataset[Row] form
    val caseClassOf = CaseClassOf.caseClassOf(schema = calculations.schema)
    var calculationsDataset: Dataset[Row] = calculations.as(caseClassOf).persist(StorageLevel.MEMORY_ONLY)

    // The names of the quantiles    
    val quantilesNames = Array("lowerWhisker", "lowerQuartile", "median", "upperQuartile", "upperWhisker")
    val indexedQuantilesNames: Array[(String, Int)] = quantilesNames.zipWithIndex

    // Split the column of quantiles into columns of distinct quantiles
    indexedQuantilesNames.foreach { case (field: String, index: Int) =>
      calculationsDataset = calculationsDataset.withColumn(field, $"quantiles".getItem(index))
    }

    // The fields required for candle sticks
    var candleFields: List[String] = classOf[Candle].getDeclaredFields.map(_.getName).toList
    candleFields = candleFields.filterNot(p => p == "$outer")

    // Hence
    calculationsDataset = calculationsDataset.select(candleFields.head, candleFields.tail: _*)
    val data: Dataset[Row] = calculationsDataset.as(CaseClassOf.caseClassOf(schema = calculationsDataset.schema))
    data.show(11)

    // Print
    val points: Array[Row] = data.collect()
    new CandleJSON().candleJSON(points = points, fileName = fileName)


  }


}
