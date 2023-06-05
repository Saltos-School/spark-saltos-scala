package com.saltos.school.spark

import org.apache.spark.sql.SparkSession

object HolaSpark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("HolaSpark")
      .master("local[2]")
      .getOrCreate()
    val count = spark.sparkContext.parallelize(1 until 1000, 40).map { i =>
        i * i
    }.reduce(_ + _)
    println(s"La suma de cuadrados del 1 al 100 es ${count}")
    spark.stop()
  }
}
