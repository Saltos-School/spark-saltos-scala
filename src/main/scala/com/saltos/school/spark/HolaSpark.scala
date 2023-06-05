package com.saltos.school.spark

import org.apache.spark.sql.SparkSession

object HolaSpark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("HolaSpark")
      .master("local[*]")
      .getOrCreate()
    val numeros = 1 until 100
    val count = spark.sparkContext.parallelize(numeros, 100).map { i =>
      Thread.sleep(1000)
      i * i
    }.reduce(_ + _)
    println(s"La suma de cuadrados del 1 al 100 es ${count}")
    spark.stop()
  }
}
