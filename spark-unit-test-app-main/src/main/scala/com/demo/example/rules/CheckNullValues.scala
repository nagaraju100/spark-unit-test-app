package com.demo.example.rules

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Check the null values in the data
 * @author nagaraju.gajula
 * created on May,27,2021
 */
object CheckNullValues {
  /**
   * Checks employee name null values
   * @param spark  Spark Session  Object to get the sql method
   * @return  dataFrame after checking the condition return the dataframe.
   */
  def getEmployeeNameNotNull(spark: SparkSession): DataFrame = {
    spark.sql("select * from employeeData where name is not null")
  }
}
