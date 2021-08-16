package com.demo.example.utils

import com.typesafe.config.{Config, ConfigFactory}

/**
 * Return the configuration values
 * @author nagaraju.gajula
 * created on May,27,2021
 */
object ConfigUtils {
  //val config = ConfigFactory.load()
  var environment = System.getProperty("APP_ENV");
  environment = if(environment !=null) environment else "dev";
  var fileName = "application-"+environment+".conf"
  println("Environment:"+environment+", property file :"+fileName)
  val config = ConfigFactory.load(fileName)
  def getConfig():Config = {
    config
  }


  def getEmployeeInputPath():String = {
    val input = config.getString(Constants.PATH_EMP_INPUT)
    input
  }


  def getEmployeeOutputPath():String = {
    val output = config.getString(Constants.PATH_EMP_OUTPUT)
    output
  }

  def getWordCountInputPath():String = {
    val input = config.getString(Constants.PATH_WC_INPUT)
    input
  }


  def getWordCountOutputPath():String = {
    val output = config.getString(Constants.PATH_WC_OUTPUT)
    output
  }

  def getEmployeeOutputFields():String = {
    val fields = config.getString(Constants.OT_EMPLOYEE_FDS)
    fields
  }

}
