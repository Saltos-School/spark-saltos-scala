package com.saltos.school.spark

import org.apache.kafka.common.config.LogLevelConfig
import org.apache.spark.sql.{Encoders, RowFactory, SparkSession}
import org.apache.spark.sql.types.DataTypes

object Movies {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    sc.setLogLevel(LogLevelConfig.WARN_LOG_LEVEL)

    val dataPath = "/home/csaltos/Documents/ml-latest-small/"

    val moviesDF = loadMoviesDF(spark, dataPath)
    moviesDF.show()
    moviesDF.printSchema()

    val ratingsDF = loadRatingsDF(spark, dataPath)
    ratingsDF.show()
    ratingsDF.printSchema()

    spark.stop()
  }

  def loadMoviesDF(spark: SparkSession, dataPath: String) = {
    val movieIdField = DataTypes.createStructField("movieId", DataTypes.LongType, true)
    val titleField = DataTypes.createStructField("title", DataTypes.StringType, true)
    val genresField = DataTypes.createStructField("genres", DataTypes.StringType, true)
    val moviesSchema = DataTypes.createStructType(Array(movieIdField, titleField, genresField))
    val moviesDF = spark.read.option("header", true).schema(moviesSchema).csv(dataPath + "movies.csv")
    moviesDF
  }

  def loadRatingsDF(spark: SparkSession, dataPath: String) = {
    val movieIdField = DataTypes.createStructField("movieId", DataTypes.LongType, true)
    val userIdField = DataTypes.createStructField("userId", DataTypes.LongType, true)
    val ratingField = DataTypes.createStructField("rating", DataTypes.DoubleType, true)
    val timestampField = DataTypes.createStructField("timestamp", DataTypes.LongType, true)
    val ratingsSchema = DataTypes.createStructType(Array(userIdField, movieIdField, ratingField, timestampField))
    val ratingsDF = spark.read.option("header", true).schema(ratingsSchema).csv(dataPath + "ratings.csv")
    ratingsDF
  }

}
