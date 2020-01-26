package com.grey.flow

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object App {

  private val localSettings = new LocalSettings()


  def main(args: Array[String]): Unit = {

    // Minimise Spark & Logger Information Outputs
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    // Spark Session Instance
    // .config("spark.master", "local")
    val spark: SparkSession = SparkSession.builder().appName("networks")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.master", "local")
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
    spark.sparkContext.setCheckpointDir("/tmp")


    // Prepare local directories
    val appDirectories = new AppDirectories()
    val dataDir = appDirectories.localDirectories(localSettings.dataDirectory)
    val warehouseDir = appDirectories.localDirectories(localSettings.warehouseDirectory)

    if (dataDir.isSuccess && warehouseDir.isSuccess) {
      val dataSteps = new DataSteps(spark)
      dataSteps.dataSteps()
    } else {
      sys.error("Unable to set-up local directories")
    }







  }

}
