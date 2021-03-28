package com.grey.source

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try
import scala.util.control.Exception

class DataRead(spark: SparkSession) {
  
  def dataRead(unloadString: String): Try[DataFrame] = {

    // Read
    val read  = Exception.allCatch.withTry(
      spark.read.json(unloadString)
    )
    
    // Hence
    if (read.isFailure) {
      sys.error(read.failed.get.getMessage)
    } else {
      read
    }    
    
  }
  
}
