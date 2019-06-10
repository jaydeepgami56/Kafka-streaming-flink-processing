package com.table.api.example

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._


/**
  * Simple example for demonstrating the use of the Table API for a Word Count in Scala.
  *
  * This example shows how to:
  *  - Convert DataSets to Tables
  *  - Apply group, aggregate, select, and filter operations
  *
  */

//Reference link : https://github.com/apache/flink/blob/master/flink-examples/flink-examples-table/src/main/scala/org/apache/flink/table/examples/scala/WordCountTable.scala
object WordCountTable {

  def main(args: Array[String]): Unit = {

    // set up execution environment for running batch SQL query.Incase if we have to use steaming environment change the ExecutionEnvironment - >StreamExecutionEnvironment
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    //create the dataset from element using tuple
    val input = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1))

    //conver the dataset to table
    val expr = input.toTable(tEnv)

    //perform Table API directly on table(DataSet DSL) to get corresponding result and again convert table to DataSet
    val result = expr
      .groupBy('word)
      .select('word, 'frequency.sum as 'frequency)
      .toDataSet[WC]

    //print the result
    result.print()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class WC(word: String, frequency: Long)

}

