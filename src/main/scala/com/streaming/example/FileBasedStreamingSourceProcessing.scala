package com.streaming.example


import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Reference link :
  *
  * https://ci.apache.org/projects/flink/flink-docs-master/dev/datastream_api.html#data-sources
  */


/**
  * How to run the code :
  * Before running code change the location of file path which is configure in code(e.g : change the variable name :inputFileName path in code)
  */
object FileBasedStreamingCount {


    def main(args: Array[String]): Unit = {

    //Create the streaming execution environment for processing data
     val env = StreamExecutionEnvironment.getExecutionEnvironment

      //read the input file from resource location and generate stream object
      val inputFileName = "D:\\Flink_Project\\kafka-flink-stream-processing\\src\\main\\resources\\Test_Summary_Report_Sample.json"

    //add file based stream source event which will read data from file and generate string
    val customSource = env.readTextFile(inputFileName)

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
