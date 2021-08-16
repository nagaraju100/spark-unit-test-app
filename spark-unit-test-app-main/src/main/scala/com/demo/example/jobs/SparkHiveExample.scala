package com.demo.example.jobs

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

/**
 * Spark Hive Example
 * @author nagaraju.gajula
 * created on May,27,2021
 */
object SparkHiveExample extends App {
  @transient lazy val log = LogManager.getLogger(getClass.getName)
  log.info("SparkHiveExample job started...")

  val spark = SparkSession
    .builder
    .appName("Spark Hive Example")
    .config("spark.sql.warehouse.dir","/user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate()
  log.info("start: databases......")
  spark.sql("show databases").show()
  log.info("SparkHiveExample job completed...")

}
