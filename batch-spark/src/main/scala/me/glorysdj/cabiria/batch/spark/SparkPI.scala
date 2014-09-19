/**
 *
 */
package me.glorysdj.cabiria.batch.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author dongjie.shi
 *
 */
object SparkPI {

  //calculate pi with leibniz
  //spark-submit --class me.glorysdj.cabiria.batch.spark.SparkPI --deploy-mode client --master spark://ford-267163.phx-os1.stratus.dev.ebay.com:7077 /root/cabiria/batch-spark_2.10-0.1.jar
  //spark-submit --class me.glorysdj.cabiria.batch.spark.SparkPI --deploy-mode cluster --master yarn /root/cabiria/batch-spark_2.10-0.1.jar
  def main(args: Array[String]) {
     if (args.length != 1) {
      System.err.println("Usage: SparkPI <in>")
      System.exit(1)
    }
    val in = args(0)
    
    val conf = new SparkConf().setAppName("SparkPI").set("spark.executor.memory", "3g")
    val spark = new SparkContext(conf)
    
    val numed = spark.textFile(in).map(_.toInt)
    val sumed = numed.map(i => math.pow(-1, i - 1) * (1.0 / (i * 2 - 1))).reduce(_ + _)
    println("PI is roughly " + 4.0 * sumed)
    spark.stop()
  }
}