package com.saltos.school.spark

import org.apache.spark.sql.{Encoders, SparkSession}

object HolaSpark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("HolaSpark")
      .getOrCreate()
    val numeros = 1 until 100
    val count = spark.sparkContext.parallelize(numeros, 100).map { i =>
      Thread.sleep(1000)
      i * i
    }.reduce { _ + _ }
    val resultado = s"La suma de cuadrados del 1 al 100 es ${count}"
    println(resultado)
    val resultadoDS = spark.createDataset(Seq(resultado))(Encoders.STRING)
    resultadoDS.write.json("s3://spark-saltos-school-data/resultado")

    spark.stop()
  }
}
