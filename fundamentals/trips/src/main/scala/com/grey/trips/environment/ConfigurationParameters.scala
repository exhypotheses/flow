package com.grey.trips.environment

class ConfigurationParameters {

  // Spark Configurations Parameters
  // If
  //    m4.2xLarge Master Node, multiples of 16 because there are 16 cores available
  //    a local 8 core machine, multiples of 8
  // sql.shuffle.partitions: The number of shuffle partitions for joins & aggregation
  // default.parallelism: The default number of partitions delivered after a transformation
  val nShufflePartitions = 2
  val nParallelism = 2

  // Hive Configuration Parameters
  // Table: Partitioning
  val partitionVariables: Map[String, String] = Map("starting" -> "DATE")

}
