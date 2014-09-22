/**
 *
 */
package me.glorysdj.cabiria.batch.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * @author dongjie.shi
 *
 */
object WordCount {

  //spark-submit --class me.glorysdj.cabiria.batch.spark.WordCount --deploy-mode client --master spark://ford-267163.phx-os1.stratus.dev.ebay.com:7077 /root/cabiria/batch-spark_2.10-0.1.jar hdfs://ford-267163.phx-os1.stratus.dev.ebay.com:8020/user/root/input/novels
  //spark-submit --class me.glorysdj.cabiria.batch.spark.WordCount --deploy-mode cluster --master yarn /root/cabiria/batch-spark_2.10-0.1.jar
  def main(args: Array[String]) {
    if (args.length != 1) {
      System.err.println("Usage: WordCount <in>")
      System.exit(1)
    }
    val in = args(0)

    val conf = new SparkConf().setAppName("WordCount").set("spark.executor.memory", "3g")
    val spark = new SparkContext(conf)

    val lines = spark.textFile(in)
    val words = lines.flatMap(_.split("\\W")).filter(!_.equals(""))
    val counts = words.map((_, 1l)).reduceByKey(_ + _).sortByKey(true)
    counts.collect.foreach(println)

    spark.stop()
  }
}