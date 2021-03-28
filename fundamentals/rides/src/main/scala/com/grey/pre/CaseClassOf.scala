package com.grey.pre

import com.grey.types.ScalaDataType
import org.apache.spark.sql.types.StructType

object CaseClassOf {

  private val scalaDataType = new ScalaDataType()

  def caseClassOf(schema: StructType): String = {

    val definitions: Seq[String] = schema.map { variable =>
      variable.name + ": " + scalaDataType.scalaDataType(dataTypeOfVariable = variable.dataType)
    }

    s"""
       |case class DataClass (
       |  ${definitions.mkString(",\n")}
       |)
     """.stripMargin

  }

}
