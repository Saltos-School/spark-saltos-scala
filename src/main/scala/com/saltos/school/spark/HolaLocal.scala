package com.saltos.school.spark

object HolaLocal {
  def main(args: Array[String]): Unit = {
    // val cuadrados = ((1 until 1000).par) map { i => i * i }
    val cuadrados = (1 until 1000) map { i => i * i }
    // val total = cuadrados.sum
    val total = (cuadrados fold 0) (_ + _)
    println("El total de cuadrados de 1 a 1000 es " + total)
  }
}
