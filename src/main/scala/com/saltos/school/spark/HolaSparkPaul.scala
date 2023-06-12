package com.saltos.school.spark

import org.apache.spark.sql.{Encoders, SparkSession}

object HolaSparkPaul {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName(getClass.getSimpleName)
      .getOrCreate()
    val numeros = 1 until 100
    val count = spark.sparkContext.parallelize(numeros, 100).map { i =>
      Thread.sleep(1000)
      i * i
    }.reduce { _ + _ }

    val resultRDD = spark.sparkContext.parallelize(Seq(s"La suma de cuadrados del 1 al 100 es ${count}"))

    resultRDD.saveAsTextFile("s3://spark-saltos-school-data/text/output/result")

    val resultDS = spark.createDataset(Seq(s"La suma de cuadrados del 1 al 100 es ${count}"))(Encoders.STRING)

    resultDS.write.json("s3://spark-saltos-school-data/hola/output/result")

    spark.stop()
  }
}
