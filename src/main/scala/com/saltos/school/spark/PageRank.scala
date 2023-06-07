package com.saltos.school.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object PageRank {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("PageRank")
      .master("local[*]")
      .getOrCreate()

    val enlacesDirectoRDD = spark.sparkContext.textFile(
      "/home/csaltos/Documents/spark-saltos-scala-main/src/main/resources/page_rank_sample01.txt")
    val enlacesDS = spark.read.textFile(
      "/home/csaltos/Documents/spark-saltos-scala-main/src/main/resources/page_rank_sample01.txt")
    val enlacesTransformadoRDD = enlacesDS.rdd

    // enlacesDirectoRDD.take(5).collect()
    // enlacesDirectoRDD.takeSample()

    val enlacesTupla: RDD[(String, String)] = enlacesDirectoRDD map { enlace =>
      val campos = enlace.split(" ")
      (campos(0), campos(1))
    }

    val enlacesAgrupadoRDD = enlacesTupla.distinct().groupByKey().persist(StorageLevel.MEMORY_ONLY)

    println("DATOS:")
    enlacesTupla.collect().map(println)

    println("AGRUPADO:")
    enlacesAgrupadoRDD.collect().map(println)

    val rankingRDDConMap = enlacesAgrupadoRDD map { case (key, value) =>
      (key, 1.0)
    }
    var rankingRDD = enlacesAgrupadoRDD.mapValues(v => 1.0)

    println("RANKING INICIAL:")
    rankingRDD.collect().foreach(println)

    for (i <- 1 to 10) {
      val enlacesTuplaInvertido = enlacesTupla map { case (key, value) =>
        (value, key)
      }
      val enlacesTuplaSwap = enlacesTupla map (_.swap)
      val pesoRDD = enlacesAgrupadoRDD.join(rankingRDD)

      println("PESOS:")
      pesoRDD.collect().foreach(println)

      println("PESOS VALORES:")
      val pesoValoresRDD = pesoRDD.values
      pesoValoresRDD.collect().foreach(println)

      println("CONTRIBUCIONES:")
      val contribucionRDD = pesoValoresRDD.flatMap { case (urlsReferidos, rank) =>
        urlsReferidos.map(url => (url, rank / urlsReferidos.size))
      }
      contribucionRDD.collect().foreach(println)
      // contribucionRDD.groupByKey()
      rankingRDD = contribucionRDD.reduceByKey(_ + _).mapValues(rank => 0.15 + 0.85 * rank)
    }

    println("RANKING CALCULADO:")
    rankingRDD.collect().foreach(println)

    spark.stop()
  }
}
