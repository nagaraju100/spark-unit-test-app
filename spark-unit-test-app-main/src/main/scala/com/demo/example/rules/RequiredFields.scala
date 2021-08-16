package com.demo.example.rules

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Returning the required fields
 * @author nagaraju.gajula
 * created on May,27,2021
 */
object RequiredFields {
  /**
   * Employee required output fields
   * @param spark  Spark Session to to get sql method
   * @param requiredFields  The required fields for employee
   * @return dataFrame
   */
    def employeeOutputFields(spark:SparkSession, requiredFields:String):DataFrame = {
      val query = "select "+requiredFields+" from employeeData"
      spark.sql(query)
    }

}
