package com.streaming.example

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.util.Random

object CustomSourceExample {

  def generateRandomStringSource(out:SourceContext[String]) = {
    val lines = Array("how are you",
      "i am processing as part of flink programming",
      " What is score",
      "I am working on saturday",
      "Did you watched yesterday's matchu",
      "are you planning to come to office?",
      "How was your trip",
      "What are you not playing?")


    while (true) {
      val index = Random.nextInt(lines.length)
      Thread.sleep(200)
      out.collect(lines(index))
    }
  }

  def main(args: Array[String]) {

    //Create the streaming execution environment for processing data
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //add custom source to event which will generate random string after every 2 min
    val customSource = env.addSource(generateRandomStringSource _)

    //perform the word count operation on each 5 second data
    val counts = customSource.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)

    //print the result
    counts.print()
    env.execute()


  }
}
