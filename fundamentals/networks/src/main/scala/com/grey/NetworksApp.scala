package com.grey

import com.grey.environment.{DataDirectories, LocalSettings}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.parallel.immutable.ParSeq
import scala.util.Try

/**
 * @author ${user.name}
 */
object NetworksApp {

  private val localSettings = new LocalSettings()
  private val dataDirectories = new DataDirectories()
  
  def main(args : Array[String]): Unit = {

    // Minimising Information Prints
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    // Spark Session Instance
    // .config("spark.master", "local")
    val spark = SparkSession.builder()
      .appName("Networks")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    // Spark Context Level Logging
    spark.sparkContext.setLogLevel("ERROR")


    // Configurations Parameters
    // Thus far a m4.2xLarge Master Node has been used. There are 16 cores available
    val nShufflePartitions = 32
    val nParallelism = 32


    // Configurations
    // sql.shuffle.partitions: The number of shuffle partitions for joins & aggregation
    // default.parallelism: The default number of partitions delivered after a transformation
    // spark.conf.set("spark.speculation", value = false)
    spark.conf.set("spark.sql.shuffle.partitions", nShufflePartitions.toString)
    spark.conf.set("spark.default.parallelism", nParallelism.toString)
    spark.conf.set("spark.kryoserializer.buffer.max", "2048m")


    // Graphs Model Checkpoint Directory
    val directories: ParSeq[Try[Boolean]] = List(localSettings.warehouseDirectory, localSettings.graphsDirectory)
      .par.map(directory => dataDirectories.localDirectoryReset(directoryName = directory))

    if (directories.head.isSuccess) {
      spark.sparkContext.setCheckpointDir(localSettings.graphsDirectory)
      new DataSteps(spark = spark).dataSteps()
    } else {
      sys.error(directories.head.failed.get.getMessage)
    }





  }

}
