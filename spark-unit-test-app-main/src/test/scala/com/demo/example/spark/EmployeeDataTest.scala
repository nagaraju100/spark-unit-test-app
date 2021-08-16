package com.demo.example.spark

import com.demo.example.jobs.EmployeeData
import com.demo.example.utils.ConfigUtils
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.test
import org.scalatest.FunSuite

/** Employee Data Test
 * @author nagaraju.gajula
 * created on May,27,2021
 */
class EmployeeDataTest extends FunSuite with SharedSparkContext with DataFrameSuiteBase {

  //var spark: SparkSession = _
  var employee: EmployeeData = _

  override def beforeAll(): Unit = {
    super.beforeAll()
   // spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    employee = new EmployeeData(spark)

  }
  test("Save the csv data by  checking the name not null") {
    employee.checkNameNullAndSave()
  }
  test("Save the required fields") {
    employee.saveSomeFields()
  }
  test("Check Total Employee Salary") {
    employee.totalSalaryOfEmployee()
  }
  test("Sample DataFrame Equals Check") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val input1 = sc.parallelize(List(1, 2, 3)).toDF
    assertDataFrameEquals(input1, input1) // equal

    val input2 = sc.parallelize(List(4, 5, 6)).toDF
    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameEquals(input1, input2) // not equal
    }
  }


  override def afterAll(): Unit = super.afterAll()


}
