package com.batch.example

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/*

How to run the code: scala <Program_name> <Input file>
Before running code specify file as a argument: D:\Flink_Project\FlinkScalaExample\src\main\resources\Test_Summary_Report_Sample.json

 */

object FileWordCount {

  def main(args: Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment

    // get input data
    //val text = env.readTextFile("file:///D:\\Flink_Project\\FlinkScalaExample\\src\\main\\resources\\Test_Summary_Report_Sample.json")
    val text = env.readTextFile("file:///" + args(0))

    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    //val outputPath = "WordCountResult"
    val outputPath = args(1)

    counts.writeAsCsv(outputPath, "\n", " ")

    env.execute("Scala WordCount Example")
  }
}
