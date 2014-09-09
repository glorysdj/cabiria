package me.glorysdj.cabiria.batch.spark

import java.io.PrintWriter
import java.io.File

object utils extends App {
  val n = 100000000
  (0 to n / 10000).foreach(i => {
    val fileName = "pi" + i
    val fileWriter = new PrintWriter(new File(fileName))
    (1 to 10000).foreach(j => {
      fileWriter.write((i * 10000 + j) + "\n")
    })
    fileWriter.close
  })
}