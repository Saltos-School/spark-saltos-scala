package com.saltos.school.spark

import org.apache.spark.sql.{SaveMode, SparkSession}

object HolaSparkEMR {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName(getClass.getSimpleName)
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val ratingsDF = spark.read.option("header", true).csv("s3://spark-saltos-school-data/ml-25m/ratings.csv")

    ratingsDF.write.mode(SaveMode.Overwrite).json("s3://spark-saltos-school-data/ml-25m/output/ratings")

    spark.stop()
  }
}
