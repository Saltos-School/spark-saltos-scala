package com.saltos.school.spark

import org.apache.kafka.common.config.LogLevelConfig
import org.apache.spark.sql.SparkSession

object HolaStreams {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("HolaSpark")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    sc.setLogLevel(LogLevelConfig.WARN_LOG_LEVEL)

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val words = lines.as[String].flatMap(_.split(" "))

    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

    spark.stop()
  }
}
