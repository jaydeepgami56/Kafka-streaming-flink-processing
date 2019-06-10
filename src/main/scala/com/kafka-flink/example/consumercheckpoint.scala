ackage kafkastreaming

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09


object consumer {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.common.restartstrategy.RestartStrategies
    env.getConfig.disableSysoutLogging
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))


    env.enableCheckpointing(5000)
    import org.apache.flink.streaming.api.CheckpointingMode
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    env.getCheckpointConfig.setCheckpointTimeout(1000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)


    val parameterTool = ParameterTool.fromArgs(args)
 
    val stream= env.addSource(new FlinkKafkaConsumer09[String](parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties()))
    val bucketpath="/home/ec2-user/jdstack23.json"
 

   print("start")
    stream.writeAsText(bucketpath, WriteMode.OVERWRITE)
    print("end")

    env.execute("consumer")

  }
}
