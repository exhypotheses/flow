package com.grey.environment

class ConfigurationParameters {

  /**
    * Spark Configurations Parameters
    * If
    *    - m4.2xLarge Master Node: multiples of 16 because there are 16 cores available
    *    - a local 8 core machine: multiples of 8
    * sql.shuffle.partitions: The number of shuffle partitions for joins & aggregation
    * default.parallelism: The default number of partitions delivered after a transformation
    */
  val nShufflePartitions = 4
  val nParallelism = 4

}
