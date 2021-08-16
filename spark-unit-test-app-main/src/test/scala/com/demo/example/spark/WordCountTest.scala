package com.demo.example.spark

import com.demo.example.jobs.WordCount
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite

/** Word Count test
 * @author nagaraju.gajula
 * created on May,27,2021
 */
class WordCountTest extends FunSuite with SharedSparkContext{

  val fileLines = Array("Line One", "Line Two", "Line Three", "Line Four")

  test("splitFile should split the file into words"){
    val inputRDD: RDD[String] = sc.parallelize[String](fileLines)
    val wordsRDD = WordCount.splitFile(inputRDD)
    assert(wordsRDD.count() == 8)
  }

  test("countWordsInFile should count words") {
    val inputRDD: RDD[String] = sc.parallelize[String](fileLines)
    val results = WordCount.countWordsInFile(inputRDD).collect
    assert(results.contains(("Line", 4)))
  }

}
