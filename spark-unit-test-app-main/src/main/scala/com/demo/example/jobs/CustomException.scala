
package com.demo.example.jobs

import com.demo.example.rules.Duplicates
import com.demo.example.utils.SparkBase
import org.apache.log4j.LogManager

import scala.util.{Failure, Success, Try}

/**
 * Checking the Custom exception
 *
 * @author nagaraju.gajula
 * created on May,27,2021
 */

class CustomException {
  //  sparkSession object
  val spark = SparkBase.getSparkSession("CustomExeption")
  // spark implicits

  import spark.implicits._

  // columns
  val columns = Seq("Id", "Name", "Salary")
  // create an data frame
  val empDuplicateData = Seq(("1", "Nagaraju", 2000), ("2", "Ravi", 8000), ("1", "Nagaraju", 3000), ("3", "Gopal", 9000)).toDF(columns: _*)
  val empData = Seq(("1", "Nagaraju", 2000), ("2", "Ravi", 8000), ("4", "Sam", 3000), ("3", "Gopal", 9000)).toDF(columns: _*)
  val duplicateCheckColumns = Seq("Id", "Name")

  def completeErrorMessage(): Unit = {
    // Complete error message
    Duplicates.checkDuplicates(empData, duplicateCheckColumns).show()
    Duplicates.checkDuplicates(empDuplicateData, duplicateCheckColumns).show()
  }

  def isDFDuplicate(): Unit = {
    // Check whether df has duplicates or not
    println("is DF  does not have duplicates::" + Try(Duplicates.checkDuplicates(empDuplicateData, duplicateCheckColumns)).isSuccess)
    println("is DF does not have duplicates::" + Try(Duplicates.checkDuplicates(empData, duplicateCheckColumns)).isSuccess)

  }

  def customErrorMessage(): Unit = {
    // Custom error message showing.
    //val finalDF = Try(Duplicates.checkDuplicates(empData,duplicateCheckColumns))
    val finalDF = Try(Duplicates.checkDuplicates(empDuplicateData, duplicateCheckColumns))
    //pattern matching
    finalDF match {
      case Success(df) => df.show()
      case Failure(exception) => println(s"Exception::=>${exception.getMessage}")
    }
  }


}

object CustomException extends App {
  @transient lazy val log = LogManager.getLogger(getClass.getName)
  log.info("CustomException job started...")

  val customException = new CustomException
  customException.isDFDuplicate()
  customException.customErrorMessage()
  customException.completeErrorMessage()
  log.info("CustomException job end...")
}
