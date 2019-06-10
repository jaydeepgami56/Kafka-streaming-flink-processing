package com.demo.example

object ResourceAccessExample {


  def main(args: Array[String]): Unit = {

    print("Started reading data from resource path")

    val propertiesFiles  =  getClass.getResource("Test_Summary_Report_Sample.json")
    println("Properties path " + propertiesFiles)

  }
}
