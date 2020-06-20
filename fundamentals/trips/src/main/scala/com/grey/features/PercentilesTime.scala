package com.grey.features

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql._

class PercentilesTime(spark: SparkSession) {

  def percentilesTime(dailyRecords: DataFrame, fields: List[String], partitionFields: List[Column],
                  orderFields: List[Column] = List()): Unit = {

    import spark.implicits._

    // Percentiles
    val windowSpec = Window.partitionBy(partitionFields: _*).orderBy(orderFields: _*)


    //stat.approxQuantile("duration", Array(0.1, 0.25, 0.5, 0.75, 0.9), 0)


  }

}
