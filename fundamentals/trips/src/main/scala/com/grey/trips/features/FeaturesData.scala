package com.grey.trips.features

import java.sql.Date

import com.grey.trips.environment.{ConfigParameters, LocalSettings}
import com.grey.trips.functions.DataWrite
import com.grey.trips.hive.{HiveBaseProperties, HiveLayerSettings}
import org.apache.spark.sql._

import scala.collection.parallel.mutable.ParArray
import scala.util.Try


class FeaturesData(spark: SparkSession) {

  private val dataWrite = new DataWrite(spark)

  private val configParameters = new ConfigParameters()
  private val localSettings = new LocalSettings()

  private val hiveBaseProperties = new HiveBaseProperties().hiveBaseProperties
  private val hiveLayerSettings = new HiveLayerSettings(spark, hiveBaseProperties)


  def featuresData(minimal: DataFrame, name: String): Try[Unit] = {

    // This import is required for (a) the $-notation, (b) implicit conversions such as converting a RDD
    // to a DataFrame, (c) encoders for [most] types, which are also automatically provided by
    // via spark.implicits._
    import spark.implicits._


    // Days
    val listOf: Array[Date] = minimal.select($"start_date").distinct()
      .map(x => x.getAs[Date]("start_date")).collect()


    // Per day, i.e., date
    val details: ParArray[Try[Unit]] = listOf.par.map { date =>

      // Initially, spark will write the day's data to 'src'.  Subsequently, the actual
      // data files - there are a few supplementary files - are transferred to a directory
      // within a hive data hub, i.e., 'dst'
      val src: String = localSettings.localWarehouse + date.toString + localSettings.localSeparator
      val dst: String = localSettings.warehouseDirectory + date.toString + localSettings.localSeparator
      val partition: String = localSettings.tableDirectory + date.toString + "/"

      // For the date in question
      var daily: Dataset[Row] = minimal.filter($"start_date" === date)
      daily = daily.repartition(numPartitions = configParameters.nParallelism, partitionExprs = $"start_station_id")
        .sortWithinPartitions($"start_station_id", $"started_at")

      // Write to file
      val write = dataWrite.dataWrite(daily = daily, date = date, src = src)

      // Hive
      if (write.isSuccess) {
        hiveLayerSettings.hiveLayerSettings(date = date, src = src, dst = dst, partition = partition)
      } else {
        // Superfluous
        sys.error(write.failed.get.getMessage)
      }

    }
    details.last

  }

}
