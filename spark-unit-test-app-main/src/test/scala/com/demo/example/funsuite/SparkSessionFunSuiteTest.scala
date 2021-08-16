package com.demo.example.funsuite

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSuite}
/**
 * Example Test Cases in FunSuite
 * @author nagaraju.gajula
 * created on May,27,2021
 */
class SparkSessionFunSuiteTest extends FunSuite with BeforeAndAfterEach {

  var sparkSession : SparkSession = _
  override
  def beforeEach() {
    sparkSession = SparkSession.builder().appName("Spark Session Test")
      .master("local")
      .config("", "")
      .getOrCreate()
  }

  test("Example"){
    //your unit test assert here like below
    assert("True".toLowerCase == "true")
  }

  override
  def afterEach() {
    sparkSession.stop()
  }
}