package com.grey.trips.source

import java.sql.Date

import com.grey.trips.environment.{ConfigurationParameters, LocalSettings}
import com.grey.trips.hive.{HiveBaseProperties, HiveBaseSettings, HiveLayerSettings}
import org.apache.spark.sql._

import scala.collection.parallel.mutable.ParArray
import scala.util.Try
import scala.util.control.Exception


class Restructure(spark: SparkSession) {

  private val configParameters = new ConfigurationParameters()
  private val localSettings = new LocalSettings()

  private val fieldsOfInterest: Array[String] = new HiveBaseSettings(spark).fieldsOfInterest
  private val hiveBaseProperties = new HiveBaseProperties().hiveBaseProperties
  private val hiveLayerSettings = new HiveLayerSettings(spark, hiveBaseProperties)


  def restructure(minimal: DataFrame, name: String): Try[Unit] = {

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
      val dst: String = localSettings.tablePath + date.toString + localSettings.localSeparator
      val partition: String = localSettings.tableDirectory + date.toString + "/"

      // For the date in question
      var daily: Dataset[Row] = minimal.filter($"start_date" === date)
      daily = daily.repartition(numPartitions = configParameters.nParallelism, partitionExprs = $"start_station_id")
        .sortWithinPartitions($"start_station_id", $"started_at")


      // Beware of time stamps, e.g., "yyyy-MM-dd HH:mm:ss.SSSXXXZ"
      val stream: Try[Unit] = Exception.allCatch.withTry(
        daily.selectExpr(fieldsOfInterest: _*).write
          .option("header", "false")
          .option("encoding", "UTF-8")
          .option("timestampFormat", localSettings.projectTimeStamp)
          .csv(src)
      )

      // Hive
      if (stream.isSuccess) {
        hiveLayerSettings.hiveLayerSettings(date = date, src = src, dst = dst, partition = partition)
      } else {
        // Superfluous
        sys.error(stream.failed.get.getMessage)
      }

    }
    details.last

  }

}
