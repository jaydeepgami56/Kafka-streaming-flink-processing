package com.streaming.example

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/*
How to run the code :
1) Open the terminal and run following command to start port listener
nc -l 9000

After running above command, command prompt will available to send data to port 9000

Reference link:https://ci.apache.org/projects/flink/flink-docs-stable/tutorials/local_setup.html
https://ci.apache.org/projects/flink/flink-docs-stable/ops/state/large_state_tuning.html
 */

object SocketTextStreamWordCount {


  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // create a stream using socket

    val socketStream = env.socketTextStream("localhost",9000)

    // implement word count

    val wordsStream = socketStream.flatMap(value => value.split("\\s+")).map(value => (value,1))

    val keyValuePair = wordsStream.keyBy(0).timeWindow(Time.seconds(15))

    val countStream = keyValuePair.sum(1)

    countStream.print()

    env.execute()
  }
}
