/**
 *
 */
package me.glorysdj.cabiria.batch.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.math
import me.glorysdj.cabiria.batch.spark.Conventions._

/**
 * @author dongjie.shi
 *
 */
object SparkPI {

  //calculate pi with leibniz
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkPI <n>")
      System.exit(1)
    }
    val n = args(0).toInt

    val begin = System.currentTimeMillis()
    val name = "SparkPI @" + DATE_FORMAT.format(begin)
    val conf = new SparkConf().setAppName(name).set("spark.executor.memory", "3g")
    val spark = new SparkContext(conf)

    val slices = n / 10000
    val numed = spark.parallelize(1 to n, slices)
    val sumed = numed.map(i => math.pow(-1, i - 1) * (1.0 / (i * 2 - 1))).reduce(_ + _)
    println("PI is roughly " + 4.0 * sumed)

    val sparked = System.currentTimeMillis()
    println("=== spark phase cost " + (sparked - begin) + "ms")
    spark.stop()
  }
}