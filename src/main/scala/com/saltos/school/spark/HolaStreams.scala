package com.saltos.school.spark

import org.apache.kafka.common.config.LogLevelConfig
import org.apache.spark.sql.{SaveMode, SparkSession}

object HolaStreams {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName(getClass.getSimpleName)
      .getOrCreate()
    val sc = spark.sparkContext

    sc.setLogLevel(LogLevelConfig.WARN_LOG_LEVEL)

    import spark.implicits._

    val lineas = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val palabras = lineas.as[String].flatMap(_.split(" "))

    val conteo = palabras.groupBy("value").count()

    val flujo = conteo.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    flujo.awaitTermination()

    spark.stop()
  }
}
