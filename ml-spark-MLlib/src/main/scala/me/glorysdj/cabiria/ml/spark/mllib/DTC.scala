/**
 *
 */
package me.glorysdj.cabiria.ml.spark.mllib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * @author dongjie.shi
 *
 */
object DTC {

  //spark-submit --class me.glorysdj.cabiria.ml.spark.mllib.DecisionTreeClassification --deploy-mode client --master spark://ford-267163.phx-os1.stratus.dev.ebay.com:7077 /root/cabiria/ml-spark-mllib_2.10-0.1.jar
  //spark-submit --class me.glorysdj.cabiria.ml.spark.mllib.DecisionTreeClassification --deploy-mode cluster --master yarn /root/cabiria/ml-spark-mllib_2.10-0.1.jar
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DecisionTreeClassification").set("spark.executor.memory", "3g")
    val spark = new SparkContext(conf)
    val data = spark.textFile("hdfs://ford-267163.phx-os1.stratus.dev.ebay.com:8020/user/root/input/iris/")
    val points = data.map(_.split(",")).map(d => {
      LabeledPoint(labelOf(d.last), Vectors.dense(d.init.map(_.toDouble)))
    })
    points.cache
    val p = points.collect

    spark.stop()
  }

  def labelOf(label: String): Double = {
    val labels = List("Iris-setosa", "Iris-versicolor", "Iris-virginica")
    labels.indexOf(label)
  }

  def calculateShannonEntropy(points: Array[LabeledPoint]): Double = {
    val size = points.length
    val labels = points.map(p => (p.label, p)).groupBy(_._1).map(p => (p._1, p._2.size))
    labels.map(l => {
      val prob = l._2.toDouble / size
      -prob * math.log(prob) / math.log(2)
    }).sum
  }
}

object DTCApp extends App {
  val points = Array(
    LabeledPoint(0, Vectors.dense(1, 1)),
    LabeledPoint(0, Vectors.dense(1, 1)),
    LabeledPoint(1, Vectors.dense(1, 0)),
    LabeledPoint(1, Vectors.dense(0, 1)),
    LabeledPoint(1, Vectors.dense(0, 1)))
  val shannonEntropy = DTC.calculateShannonEntropy(points)
  println(shannonEntropy)
}