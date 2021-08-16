package com.demo.example.junit

import org.apache.spark.sql.SparkSession
import org.junit.{After, Before, Test}
import org.junit.Assert._

/**
 * Example Test cases in Junit
 * @author nagaraju.gajula
 * created on May,27,2021
 */

@Test
class SparkSesstionJunitTest {
  var spark: SparkSession = _

  @Before
  def beforeFunction(): Unit = {
    //spark = SessionSpark.getSparkSession()
    spark = SparkSession.builder().appName("App Name").master("local").getOrCreate()
    System.out.println("Before Function")
  }

  @After
  def afterFunction(): Unit = {
    spark.stop()
    System.out.println("After Function")
  }

  @Test
  def testRddCount() = {
    val rdd = spark.sparkContext.parallelize(List(1, 2, 3))
    val count = rdd.count()
    assertTrue(3 == count)
  }

  @Test
  def testDfNotEmpty() = {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    val numDf = spark.sparkContext.parallelize(List(1, 2, 3)).toDF("nums")
    assertFalse(numDf.head(1).isEmpty)
  }

  @Test
  def testDfEmpty() = {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    val emptyDf = spark.sqlContext.createDataset(spark.sparkContext.emptyRDD[Num])
    assertTrue(emptyDf.head(1).isEmpty)
  }
}

case class Num(id: Int)