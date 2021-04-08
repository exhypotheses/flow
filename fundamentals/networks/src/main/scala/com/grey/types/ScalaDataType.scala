package com.grey.types

import org.apache.spark.sql.types._

class ScalaDataType {

  def scalaDataType(dataTypeOfVariable: DataType): String = {

    dataTypeOfVariable match {

      case _:ByteType => "Byte"
      case _:ShortType => "Short"
      case _:IntegerType => "Int"
      case _:LongType => "Long"
      case _:FloatType => "Float"
      case _:DoubleType => "Double"
      case _:DecimalType => "java.math.BigDecimal"
      case _:StringType => "String"
      case _:BinaryType => "Array[Byte]"
      case _:BooleanType => "Boolean"
      case _:TimestampType => "java.sql.Timestamp" // Beware
      case _:DateType => "java.sql.Date"
      case _:ArrayType => "scala.collection.Seq"
      case _:StructType => "org.apache.spark.sql.Row"
      case _:MapType => "scala.collection.Map"
      case _ => "string"

    }

  }

}
