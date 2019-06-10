package com.table.api.example

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.types.Row
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.sources.{CsvTableSource, TableSource}

/**
  * Reference link :
  * https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/table/common.html#implicit-conversion-for-scala
  * https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/table/sql.html
  * https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/functions.html
  * https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/tableApi.html
  */
object CSVTableSourceExample {

  def main(args: Array[String]): Unit = {

    println("Start performing sql operation using apache flink-DataSET API")

    //create required batch execution environment for quering on csv table data
    val env = ExecutionEnvironment.getExecutionEnvironment

    // get a TableEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    //input csv file
    val inputCSVFilePath = "D:\\Flink_Project\\FlinkScalaExample\\src\\main\\resources\\employee.csv"

    // create a TableSource with required csv header information(.e.g : CsvTableSource is used to read data from csv)
    val csvSource: CsvTableSource = CsvTableSource.builder().path(inputCSVFilePath)
      .ignoreFirstLine()
      .fieldDelimiter(",")
      .field("name", Types.STRING)
      .field("age", Types.INT)
      .field("country", Types.STRING)
      .field("gender", Types.STRING)
      .field("salary", Types.STRING).build()


    // register the TableSource as table "employee" to query
    tableEnv.registerTableSource("employee", csvSource)

    //Select state wise salary information
    val salarySumDetails = tableEnv.scan("employee")
     .groupBy('country).select('country,'age.sum as 'ageSummary )

    val queryResultCsvPath = "D:\\Flink_Project\\kafka-flink-stream-processing\\src\\main\\resources\\employee_query_result.csv"

    // Convert to Dataset and display results
    println("Age wise summation for each country")
    val ds = salarySumDetails.toDataSet[Row]
    ds.print()


    //get distinct state present in data
    val distinctStateDetails = tableEnv.scan("employee")
      .groupBy('country).select('country,'country.count as 'distinct_state)

    println("Distinct state present in data")
    //convert the result in dataset and display result
    val distinctDS = distinctStateDetails.toDataSet[Row]
    distinctDS.print()


    //fetch all record where country is US
    val fetchUSCountryDetails = tableEnv.scan("employee")
      .filter('country === "US").select('*)

    println("fetch all record where country == US ")
    //covert the result in dataset and display result
    val filterDS = fetchUSCountryDetails.toDataSet[Row]
    filterDS.print()

    //filter all people whose age is less than 25
    val ageLevelFilter = tableEnv.scan("employee").
      filter('age > 25).select('*)

    println("Selecting employee based on age where age > 25")
    //covert the result in dataset and display result
    val ageFilterDS = ageLevelFilter.toDataSet[Row]
    ageFilterDS.print()

    //create sink to write query result in csv files
    // create a TableSink which will store data by | delimited,create single output file and overwrite result
    val csvSink: CsvTableSink = new CsvTableSink(queryResultCsvPath,"|",1,WriteMode.OVERWRITE)

    // define the field names and types
    val fieldNames: Array[String] = Array("name", "age", "country","gender","salary")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.INT, Types.STRING,Types.STRING,Types.STRING)

    // register the TableSink as table "CsvSinkTable"
    tableEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, csvSink)

    println("Writing csv process data in file")
    salarySumDetails.writeToSink(csvSink)


    env.execute()

    println("Completed data processing operation using flink Table API")
  }
}
