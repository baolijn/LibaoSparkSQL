package com.libao.spark

import org.apache.spark.sql.SparkSession

object SaveToHive {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("SaveToHive")
      .enableHiveSupport()
      .master("local[2]").getOrCreate()

    val userDF = spark.read.format("json").load("file:///usr/hdp/2.6.2.38-1/spark2/examples/src/main/resources/people.json")
    userDF.createOrReplaceTempView("table1")
    spark.sql("insert into people select age,name from table1")

      spark.stop()
  }
}
