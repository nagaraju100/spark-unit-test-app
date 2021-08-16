package com.demo.example.writers

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * Writes different formats of the data to given output path
 * @author nagaraju.gajula
 * created on May,27,2021
 */
object EmployeeWriter {
  /**
   * Writes the given dataframe to the given path
   * @param df  DataFame
   * @param format  Which format of the data , we need to save
   * @param path where we need to save the data.
   */
   def writeDF(df:DataFrame,format:String,path:String): Unit = {
        df.write
          .format(format)
          .option("header",true)
          .mode(SaveMode.Overwrite)
          .save(path)
   }

  /**
   * Writes the given dataframe to the given path
   * @param df  DataFame
   * @param format  Which format of the data , we need to save
   * @param path where we need to save the data.
   * @param reparations If want more number of files then we can reparation the data
   */
  def writeDF(df:DataFrame, format:String, path:String, reparations:Int): Unit = {
    writeDF(df.repartition(reparations),format,path)
  }
}
