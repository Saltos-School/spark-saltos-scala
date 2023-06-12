package com.saltos.school.spark

import org.apache.kafka.common.config.LogLevelConfig
import org.apache.spark.sql.{Encoders, RowFactory, SparkSession, Row}
import org.apache.spark.sql.types.DataTypes

case class Persona2(nombre: String, edad: Long)

object HolaDataFrames {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("HolaSpark")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val personasRDD = sc.textFile("src/main/resources/people.txt")

    // sc.setLogLevel(LogLevelConfig.WARN_LOG_LEVEL)

    personasRDD.take(5).foreach(println)

    val nombreField = DataTypes.createStructField("nombre", DataTypes.StringType, true)
    val edadField = DataTypes.createStructField("edad", DataTypes.LongType, false)
    val personaSchema = DataTypes.createStructType(Array(nombreField, edadField))

    val personasRowRDD = personasRDD.map { linea =>
      val campos = linea.split(",")
      val nombre = campos(0).trim
      val edad = campos(1).trim.toLong
      Row(nombre, edad)
    }

    val personasDF = spark.createDataFrame(personasRowRDD, personaSchema)
    personasDF.show()
    personasDF.printSchema()

    val personasEncoder = Encoders.product[Persona2]
    val personasDS = personasDF.as(personasEncoder)
    personasDS.show()
    personasDS.printSchema()

    // val mayoresDeEdadRDD = personasRDD.filter(linea => true)
    // val mayoresDeEdadRowRDD = personasRowRDD.filter(row => true)

    val mayoresDeEdadDF = personasDF.filter { personaRow =>
      // val nombre = personaRow(0)
      // val nombre2 = personaRow.get(0)
      // val nombre3 = personaRow.getAs[String]("nombre")
      // val nombre4 = personaRow.getString(0)
      val edad = personaRow.getAs[Long]("edad")
      edad > 21
    }
    // personasDF.filter("edad > 21")
    println("Mayores de edad:")
    mayoresDeEdadDF.show()

    val mayoresDeEdadDS = personasDS.filter(_.edad > 21)
    // personasDS.filter("edad > 21")
    println("Mayores de edad:")
    mayoresDeEdadDS.show()

    val personasRecordRDD = personasRDD.map { linea =>
      val campos = linea.split(",")
      val nombre = campos(0).trim
      val edad = campos(1).trim.toLong
      Persona2(nombre, edad)
    }
    val personasDS2 = personasRecordRDD.toDS()
    personasDS2.show()
    personasDS2.printSchema()

    val personasJsonDF = spark.read.json("src/main/resources/people.json")
    personasJsonDF.show()
    personasJsonDF.printSchema()

    val nameField = DataTypes.createStructField("name", DataTypes.StringType, true)
    val ageField = DataTypes.createStructField("age", DataTypes.LongType, true)
    val peopleSchema = DataTypes.createStructType(Array(ageField, nameField))

    val personasJsonDF2 = spark.createDataFrame(personasJsonDF.rdd, peopleSchema)
    personasJsonDF2.show()
    personasJsonDF.printSchema()

    spark.stop()
  }
}
