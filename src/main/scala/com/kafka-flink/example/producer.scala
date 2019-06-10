/*
name := "kafkasetup"

version := "0.1"

scalaVersion := "2.11.6"
// https://mvnrepository.com/artifact/org.apache.flink/flink-streaming-java
// https://mvnrepository.com/artifact/org.apache.flink/flink-connector-kafka-0.9
libraryDependencies += "org.apache.flink" %% "flink-connector-kafka-0.9" % "1.7.2"
// https://mvnrepository.com/artifact/org.apache.flink/flink-table
libraryDependencies += "org.apache.flink" %% "flink-table" % "1.7.2" % "provided"
// https://mvnrepository.com/artifact/org.apache.flink/flink-streaming-java
libraryDependencies += "org.apache.flink" %% "flink-streaming-java" % "1.7.2" % "provided"
// https://mvnrepository.com/artifact/org.apache.flink/flink-java
libraryDependencies += "org.apache.flink" % "flink-java" % "1.7.2"
// https://mvnrepository.com/artifact/org.apache.flink/flink-connector-kafka-0.8
libraryDependencies += "org.apache.flink" %% "flink-connector-kafka-0.8" % "1.7.2"

// https://mvnrepository.com/artifact/org.apache.flink/flink-streaming-scala
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % "1.7.2"
// https://mvnrepository.com/artifact/org.apache.flink/flink-connector-kafka-0.11
libraryDependencies += "org.apache.flink" %% "flink-connector-kafka-0.11" % "1.7.2"

*/






package jdtest1
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09

object producer {
  val bEnv = ExecutionEnvironment.getExecutionEnvironment
  //val tEnv = TableEnvironment.getTableEnvironment(bEnv)
  //val csvInput1 = bEnv.readCsvFile[Olympic]("/home/ec2-user//oly.csv",pojoFields = Array("Name","Age","Country","Year","ClosingDate","Sport","Gold","Silver","Bronze","Total"))
  //implicit val typeInfo = TypeInformation.of(classOf[Olympic])

 // val csvInput1 = bEnv.readCsvFile[Olympic]("/home/ec2-user/oly.csv",pojoFields = Array("Name","Age","Country","Year","ClosingDate","Sport","Gold","Silver","Bronze","Total"))


 /* class Olympic {
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

  import org.apache.flink.streaming.api.functions.source.SourceFunction

  @SerialVersionUID(119007289730474249L)
  class SimpleStringGenerator extends SourceFunction[String] {
    var running = true
    var i = 0

    @throws[Exception]
    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
      while ( {
        running
      }) {
        ctx.collect("FLINK-" + {
          i += 1; i - 1
        })
        Thread.sleep(10)
      }
    }

    override def cancel(): Unit = {
      running = false
    }
  }


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment




    val messageStream = env.addSource(new SimpleStringGenerator())
    import org.apache.flink.api.java.utils.ParameterTool
    val parameterTool = ParameterTool.fromArgs(args)

    messageStream.addSink(new FlinkKafkaProducer09(parameterTool.getRequired("bootstrap.servers"), parameterTool.getRequired("topic"), new SimpleStringSchema()))


    env.execute("producer")




   // val csvTableSource1=  env.addSource(new SawtoothSource(100,40,1))




  }



}
