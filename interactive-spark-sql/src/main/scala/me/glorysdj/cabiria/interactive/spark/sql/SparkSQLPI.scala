/**
 *
 */
package me.glorysdj.cabiria.interactive.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * @author dongjie.shi
 *
 */
object SparkSQLPI {

  //spark-submit --class me.glorysdj.cabiria.interactive.spark.sql.SparkSQLPI --deploy-mode client --master spark://ford-267163.phx-os1.stratus.dev.ebay.com:7077 /root/cabiria/interactive-spark-sql_2.10-0.1.jar /user/root/input/pi
  //spark-submit --class me.glorysdj.cabiria.interactive.spark.sql.SparkSQLPI --deploy-mode cluster --master yarn /root/cabiria/interactive-spark-sql_2.10-0.1.jar
  def main(args: Array[String]) {
    if (args.length != 1) {
      System.err.println("Usage: SparkSQLPI <in>")
      System.exit(1)
    }
    val in = args(0)

    val conf = new SparkConf().setAppName("SparkPI").set("spark.executor.memory", "3g")
    val spark = new SparkContext(conf)
    val sparkSQL = new org.apache.spark.sql.SQLContext(spark)

    val numed = spark.textFile(in).map(i => Num(i.toInt))
    import sparkSQL.createSchemaRDD
    numed.registerAsTable("numbers")
    val values = sparkSQL.sql("SELECT value FROM numbers")
    val pi = values.map(r => {
      val v = r(0).asInstanceOf[Int]
      math.pow(-1, v - 1) * (1.0 / (v * 2 - 1))
    }).reduce(_ + _)
    println("PI is roughly " + 4.0 * pi)
    spark.stop()
  }

}

case class Num(value: Int)