package com.grey.trips.hive

import java.sql.Date

import com.grey.libraries.HiveLayerCaseClass
import com.grey.trips.environment.ConfigurationParameters


class HiveLayerProperties {


  private val configParameters = new ConfigurationParameters()


  def hiveLayerProperties(date: Date, partition: String): HiveLayerCaseClass#HLCC = {

    // Specific
    val partitionValues: Map[String, String] = Map("starting" -> date.toString)

    // Case -> ALTER TABLE (specific)
    val partitionSpecification: String = configParameters.partitionVariables.keys.map { key =>
      key + " = '" + partitionValues(key) + "'"
    }.mkString(", ")

    // Hence
    new HiveLayerCaseClass().HLCC(partitionOfInterest = partition,
      partitionSpecification = partitionSpecification)

  }

}
