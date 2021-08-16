package com.demo.example.readers

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Reads different format data of Employee
 * @author nagaraju.gajula
 * created on May,27,2021
 */
object EmployeeReader {
  /**
   * Reads the csv data and converts into dataframe
   * @param spark  SparkSession object to read data from csv
   * @param path  From where the data needs to be read
   * @return
   */
  def readCSVToDF(spark:SparkSession,path:String):DataFrame = {
        spark.read
          .option("inferSchema",true)
          .option("header",true)
          .csv(path)
    // if needs more options ,we can provide here.
  }


}
