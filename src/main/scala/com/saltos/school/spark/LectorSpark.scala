package com.saltos.school.spark

import org.apache.spark.sql.SparkSession

object LectorSpark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("LectorSpark")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val people = sc.textFile("/home/csaltos/Documents/spark-saltos-scala-main/src/main/resources/people2.txt", 20)
    val total = people.count()
    println("El archivo people contiene:")
    people.collect().map(println)
    people.map(println)
    println("Total de lineas es: " + total)

    spark.stop()
  }
}
