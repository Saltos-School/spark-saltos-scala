package com.saltos.school.spark

import org.apache.spark.sql.{Dataset, SparkSession}

case class Persona(id: Long, nombre: String, edad: Int)
case class Cuenta(id: Long, email: String)

object HolaDataSets {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("HolaSpark")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val peopleTextDS = spark.read.textFile(
      "/home/csaltos/Documents/spark-saltos-scala-main/src/main/resources/people.txt")

    val personaDS = convertirAPersonaDS(spark, peopleTextDS).cache()

    println("Personas: ")
    personaDS.show()

    println("Personas mayores de 21 años: ")
    personaDS.filter("edad > 21").show()
    personaDS.filter(_.edad > 21).show()
    personaDS.filter(persona => persona.edad > 21).show()

    val cuentasDS = sc.parallelize(
      List(
        Cuenta(1, "pepe@prueba.com"),
        Cuenta(2, "pepe2@prueba.com"),
        Cuenta(3, "pepe3@prueba.com")
      )).toDS()

    val personasConCuentasDS = personaDS.join(cuentasDS, "id", "inner")
    personasConCuentasDS.show()

    personaDS.agg(Map("edad" -> "min")).show
    personaDS.agg(Map("edad" -> "max")).show
    personaDS.agg(Map("edad" -> "avg")).show

    personaDS.groupBy("edad").count().show

    spark.stop()
  }

  def convertirAPersonaDS(spark: SparkSession, peopleTextDS: Dataset[String]) = {
    import spark.implicits._
    val personasRDD = peopleTextDS.rdd.zipWithIndex().map { case (linea, index) =>
      val campos = linea.split(",")
      val nombre = campos(0).trim
      val edad = campos(1).trim.toInt
      Persona(index + 1, nombre, edad)
    }
    personasRDD.toDS()
  }

}
