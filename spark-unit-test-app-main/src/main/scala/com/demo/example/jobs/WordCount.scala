package com.demo.example.jobs

import java.util.Date

import com.adobe.example.utils.ConfigUtils
import com.demo.example.utils.{ConfigUtils, SparkBase}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.rdd.RDD

/**
 * Count the number of words in the given file
 * @author nagaraju.gajula
 * created on May,27,2021
 */
object WordCount {
  @transient lazy val log = LogManager.getLogger(getClass.getName)
  //log.setLevel(Level.INFO)
  def main(args: Array[String]) {
    log.info("WordCount Program Started....")
    val sc = SparkBase.getSparkContext("WordCount")
    val file = sc.textFile(ConfigUtils.getWordCountInputPath())
    countWordsInFile(file).saveAsTextFile(ConfigUtils.getWordCountOutputPath()+new Date().getMinutes)
    log.info("WordCount Program Completed....")
  }

  def countWordsInFile = splitFile _ andThen countWords _

  /**
   @param wordsByLine  Words are given in the form of lines.
   @return rdd  Returns the lines splitted rdd.
   */
  def splitFile(wordsByLine: RDD[String]): RDD[String] = {
    wordsByLine.flatMap(line => line.split(" "))
  }
  /**
  @param words  Words RDD
  @return rdd  Contains the words and its count.
   */
  def countWords(words: RDD[String]): RDD[(String, Int)] = {
    words.map(word => (word, 1)).reduceByKey(_ + _)
  }
}
