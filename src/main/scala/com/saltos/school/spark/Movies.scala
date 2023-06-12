package com.saltos.school.spark

import org.apache.kafka.common.config.LogLevelConfig
import org.apache.spark.sql.{Encoders, RowFactory, SparkSession}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.storage.StorageLevel

object Movies {
  def main(args: Array[String]): Unit = {

    val userId = if (args.isEmpty) "5" else args(0)

    val env = System.getProperty("movies.env")

    val isDevelopment = env == "dev"
    val isProduction = !isDevelopment

    val spark = SparkSession
      .builder
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    if (isProduction) {
      sc.setLogLevel(LogLevelConfig.WARN_LOG_LEVEL)
    }

    val dataPath = if (isProduction) "s3://spark-saltos-school-data/ml-25m/" else "/home/csaltos/Documents/ml-latest-small/"

    val moviesDF = loadMoviesDF(spark, dataPath).cache()
    moviesDF.show()
    moviesDF.printSchema()

    val ratingsDF = loadRatingsDF(spark, dataPath).cache()
    ratingsDF.show()
    ratingsDF.printSchema()

    val linksDF = loadLinksDF(spark, dataPath).cache()
    linksDF.show()
    linksDF.printSchema()

    val moviesWithRatings = moviesDF.join(ratingsDF, "movieId")
    moviesWithRatings.show()
    moviesWithRatings.printSchema()

    // val moviesWithRatingsAndLinks = moviesWithRatings.join(linksDF, "movieId")

    val userMoviesRatings = moviesWithRatings.filter("userId = " + userId).cache()
    userMoviesRatings.show()
    println("Ratings por usuario: " + userMoviesRatings.count())

    val userMoviesRatingsWithLinks = userMoviesRatings.join(linksDF, "movieId")

    /*userMoviesRatings flatMap { row =>
      val movieId = row.get(0)
      linksDF.filter("movieId = " + movieId) map { row =>
        row.get(2)
      }
    }*/

    import org.apache.spark.sql.functions.desc
    val userMoviesTopRatings = userMoviesRatingsWithLinks.sort(desc("rating"))
    userMoviesTopRatings.show(1000)

    val userMoviesTop10Ratings = userMoviesTopRatings.take(10)
    println("El top 10 de películas del usuario " + userId + " es:")
    userMoviesTop10Ratings.foreach { movieRow =>
      val title = movieRow.get(1)
      val rating = movieRow.getAs[Double]("rating")
      val imdbId = movieRow.getAs[String]("imdbId")
      println("Titulo: " + title + ", Estrellas: " + rating + ", Link: http://www.imdb.com/title/tt" + imdbId + "/")
    }

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

  def loadLinksDF(spark: SparkSession, dataPath: String) = {
    val movieIdField = DataTypes.createStructField("movieId", DataTypes.LongType, true)
    val imdbIdField = DataTypes.createStructField("imdbId", DataTypes.StringType, true)
    val tmdbIdField = DataTypes.createStructField("tmdbId", DataTypes.LongType, true)
    val ratingsSchema = DataTypes.createStructType(Array(movieIdField, imdbIdField, tmdbIdField))
    val ratingsDF = spark.read.option("header", true).schema(ratingsSchema).csv(dataPath + "links.csv")
    ratingsDF
  }

}
