package com.demo.example.rules

import com.demo.example.exceptions.AppException
import org.apache.spark.sql.DataFrame

/**
 * Duplicates - checks duplicate columns, remove duplicate coumns
 *
 * @author nagaraju.gajula
 * created on May,27,2021
 */
object Duplicates {
  /**
   * Checking the duplicates in DataFrame with the given column names and throws exception
   * @param df DataFrame in which ,need to find duplicates
   * @param columns column names,which are used to check the duplicates
   * @return DataFrame
   */
  def checkDuplicates(df: DataFrame,columns: Seq[String]):  DataFrame={
          if(df.dropDuplicates(columns).count() != df.count()){
            throw new AppException(s"Duplicates found in ${columns.mkString(",")}")
          }
    df
  }
}
