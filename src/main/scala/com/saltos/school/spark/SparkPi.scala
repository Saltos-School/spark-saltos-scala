package com.saltos.school.spark

import org.apache.spark.sql.SparkSession

object SparkPi {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkPi").master("local[*]").getOrCreate()
    val totalPuntos = Int.MaxValue
    val puntosDentro = spark.sparkContext.parallelize(1 until totalPuntos, 100).map { _ =>
      val x = math.random() * 2 - 1
      val y = math.random() * 2 - 1
      if (x * x + y * y <= 1) 1 else 0
    }.reduce(_ + _)
    val pi = 4.0 * puntosDentro / totalPuntos
    println("El valor aproximado de PI es: " + pi)
    spark.close()
  }
}
