package com.demo.example.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
/**
    Base Spark  - will give SparkContext, Spark Session Object.
 */
object SparkBase {
  /**
   * Get the spark Context object with the given appName
   * @param appName  Application Name
   * @return SparkContext
   * @author nagaraju.gajula
   * created on May,27,2021
   */
  def getSparkContext(appName:String): SparkContext = {
     val conf=  new SparkConf().setAppName(appName).setMaster("local[*]")
     val sc = new SparkContext(conf)
    sc
  }

  /**
   * Get the spark Session object with the given appName
   * @param appName Application Name
   * @return Spark Session
   */
  def getSparkSession(appName:String):SparkSession = {
    val spark = SparkSession.builder().
      appName(appName)
      .master("local[*]")
      .getOrCreate()
    spark

  }


}
