package com.streaming.example


import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import scala.io.Source
import scala.util.Random

/**
  * How to run the code :
  * Before running code change the location of file path which is configure in code(e.g : change the variable name :inputFileName path in code)
  */


object FileBasedStreamingCount {


  //generate random string based on file content
  def generateRandomStringSource(out:SourceContext[String]) = {

    //read the input file from resource location and generate stream object
    val inputFileName = "D:\\Flink_Project\\kafka-flink-stream-processing\\src\\main\\resources\\Test_Summary_Report_Sample.json"
    val lines = Source.fromFile(inputFileName).getLines().toList
    while (true) {
      val index = Random.nextInt(lines.length)
      Thread.sleep(200)
      out.collect(lines(index))
    }
  }

  def main(args: Array[String]): Unit = {

    //Create the streaming execution environment for processing data
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //add custom source to event which will generate random string after every 2 min
    val customSource = env.addSource(generateRandomStringSource _)

    //perform the word count operation on each 5 second data
    val counts = customSource.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(0)
      .timeWindow(Time.seconds(3))
      .sum(1)

    //print the result
    counts.print()
    env.execute()


  }
}
