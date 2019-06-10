package kafkastreaming
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09

import scala.io.Source
import scala.util.Random
object producer {

 
  val csvInput1 = "/home/ec2-user/jd.json"


  val lines12= Source.fromFile(csvInput1).getLines().toList
  class SimpleStringGenerator extends SourceFunction[String] {
    var running = true
    var i = 0

    @throws[Exception]
    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
      while ( {
        running
      }) {
        val index = Random.nextInt(lines12.length)
        ctx.collect(lines12(index))
        Thread.sleep(200)
      }
    }

    override def cancel(): Unit = {
      running = false
    }

  }
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
   
    val messageStream = env.addSource(new SimpleStringGenerator())
  
    val parameterTool = ParameterTool.fromArgs(args)
    messageStream.addSink(new FlinkKafkaProducer09[String](parameterTool.getRequired("bootstrap.servers"), parameterTool.getRequired("topic"), new SimpleStringSchema()))

    env.execute("producer")








  }





}
