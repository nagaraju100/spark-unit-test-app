package com.demo.example.jobs

import WordCount.getClass
import com.adobe.example.rules.RequiredFields
import com.adobe.example.utils.Constants
import com.demo.example.readers.EmployeeReader
import com.demo.example.rules.{CheckNullValues, RequiredFields}
import com.demo.example.utils.{ConfigUtils, Constants, SparkBase}
import com.demo.example.writers.EmployeeWriter
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
/**
 * Employee required operations will be performed.
 *
 * @param spark Spark Session Object which will be useful to create dataframe
 * @author nagaraju.gajula
 * created on May,27,2021
 */
class EmployeeData(spark:SparkSession){
 val outputPath = ConfigUtils.getEmployeeOutputPath()
 System.currentTimeMillis()
  /**
   *  Checks null names and save the data into the specified path
   */
   def checkNameNullAndSave():Unit = {
    val inputPath = ConfigUtils.getEmployeeInputPath()
    val employeeCSVData:DataFrame = EmployeeReader.readCSVToDF(spark,inputPath)
    employeeCSVData.printSchema()
    employeeCSVData.show()
    employeeCSVData.createOrReplaceTempView("employeeData")

    val employeeDataAfterNullCheck = CheckNullValues.getEmployeeNameNotNull(spark)

    EmployeeWriter.writeDF(employeeDataAfterNullCheck,Constants.CSV_FORMAT,outputPath)
   }
 /**
   Saves the some of the fields provided.
  */

 def saveSomeFields(): Unit ={
  val requiredFields = ConfigUtils.getEmployeeOutputFields()
  // Saving the required fields
  val employeeSpecifiedFields = RequiredFields.employeeOutputFields(spark,requiredFields)
  EmployeeWriter.writeDF(employeeSpecifiedFields,Constants.CSV_FORMAT,outputPath+"\\reqiredfields")

 }
 /**
 *  Check the total salary of the employees
*/
 def totalSalaryOfEmployee(): Unit ={
  // Saving the required fields
  val employeeTotalSalary = spark.sql("select sum(salary) as totalSalary from employeeData")
  employeeTotalSalary.show()
 }
}

/**
 * Job - Checking Employee Data different usecase
 */
object EmployeeData extends App {

 // if we are run app in windows.
 //System.setProperty("hadoop.home.dir", "D:\\Softwares\\hadoop-2.8.5")
 // Spark Session
 @transient lazy val log = LogManager.getLogger(getClass.getName)
 log.info("EmployeeData job started...")
  val spark  = SparkBase.getSparkSession("EmployeeData")
 val employee = new EmployeeData(spark)
 employee.checkNameNullAndSave()
 employee.saveSomeFields()
 employee.totalSalaryOfEmployee()
// Stop spark context
 spark.stop()
 log.info("EmployeeData job completed...")
}
