package com.table.api.example


import org.apache.flink.api.scala._

import org.apache.flink.table.api.scala._

import org.apache.flink.table.api.TableEnvironment

/*
Program to create dataset and perform query operation using SQL query
 */

object WordCountSQL {

  // *************************************************************************
  //     PROGRAM
  // *************************************************************************

  def main(args: Array[String]): Unit = {

    // set up execution environment for batch environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv =  TableEnvironment.getTableEnvironment(env)

    //create the dataset using class object
    val input = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1))

    // register the DataSet as table "WordCount" which help us to write sql query directly on table(SQLDSL)
    tEnv.registerDataSet("WordCount", input, 'word, 'frequency)

    // run a SQL query on the Table and retrieve the result as a new Table
    val table = tEnv.sqlQuery("SELECT word, SUM(frequency) FROM WordCount GROUP BY word")

    //convert the table to again dataset and store in WC class
    table.toDataSet[WC].print()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class WC(word: String, frequency: Long)


}
