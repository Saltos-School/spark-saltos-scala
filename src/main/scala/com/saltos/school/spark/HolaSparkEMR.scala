package com.saltos.school.spark

import org.apache.spark.sql.{Encoders, RowFactory, SaveMode, SparkSession}
import org.apache.kafka.common.config.LogLevelConfig
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.storage.StorageLevel

object HolaSparkEMR {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val dataPath = "s3://spark-saltos-school-data/ml-25m/"

    val movieIdField = DataTypes.createStructField("movieId", DataTypes.LongType, true)
    val userIdField = DataTypes.createStructField("userId", DataTypes.LongType, true)
    val ratingField = DataTypes.createStructField("rating", DataTypes.DoubleType, true)
    val timestampField = DataTypes.createStructField("timestamp", DataTypes.LongType, true)
    val ratingsSchema = DataTypes.createStructType(Array(userIdField, movieIdField, ratingField, timestampField))
    val ratingsDF = spark.read.option("header", true).schema(ratingsSchema).csv(dataPath + "ratings.csv")

    ratingsDF.write.json(dataPath + "output/ratings")

    spark.stop()
  }
}
