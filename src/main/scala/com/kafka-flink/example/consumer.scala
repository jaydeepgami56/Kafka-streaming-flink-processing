package jdtest1

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09

/*object consumer {
  class Olympic {
    var Name:String=_
    var Age:Int=_
    var Country:String=_
    var Year:String=_
    var ClosingDate:String=_
    var Sport:String=_
    var Gold:Int=_
    var Silver:Int=_
    var Bronze:Int=_
    var Total: Int =_

  }
*/
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val parameterTool = ParameterTool.fromArgs(args)
   // val props = new Properties()
    //props.setProperty("bootstrap.servers", "localhost:9092")
    //props.setProperty("zookeeper.connect", "localhost:2181")
    //props.setProperty("group.id", "md_streaming")
    implicit val typeInfo = TypeInformation.of(classOf[String])

    val datastream1= env.addSource(new FlinkKafkaConsumer09(parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties()))
    datastream1.writeAsText("/home/ec2-user/jdstack23.txt",WriteMode.OVERWRITE)
    //datastream1.print()
    env.execute("consumer")

  }

}
