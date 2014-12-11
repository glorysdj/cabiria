/**
 *
 */
package me.glorysdj.cabiria.ml.spark.mllib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.Gini

/**
 * @author dongjie.shi
 *
 */
object DTC {

  //spark-submit --class me.glorysdj.cabiria.ml.spark.mllib.DTC --deploy-mode client --master spark://ford-267163.phx-os1.stratus.dev.ebay.com:7077 /root/cabiria/ml-spark-mllib_2.10-0.1.jar
  //spark-submit --class me.glorysdj.cabiria.ml.spark.mllib.DTC --deploy-mode cluster --master yarn /root/cabiria/ml-spark-mllib_2.10-0.1.jar
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DecisionTreeClassification").set("spark.executor.memory", "3g")
    val spark = new SparkContext(conf)
    val data = spark.textFile("hdfs://ford-267163.phx-os1.stratus.dev.ebay.com:8020/user/root/input/abalone/").map(_.split(",")).cache

    val errors = List.range(1, 11).map(maxDepth => {
      val points1 = data.filter(!_.head.contains("I")).map(d => {
        LabeledPoint(label1(d.head), Vectors.dense(d.tail.map(_.toDouble)))
      })
      val model1 = DecisionTree.train(points1, Classification, Gini, maxDepth)
      val predictions = points1.map(d => {
        //val label = d.last
        //val features = Vectors.dense(d.init.map(_.toDouble))
        val prediction1 = model1.predict(d.features)
        (d.label, math.round(prediction1))
        /*
      prediction1 match {
        case 1.0 => (label, "Iris-setosa")
        case _ => {
          val prediction2 = model2.predict(Vectors.dense(d.init.map(_.toDouble)))
          prediction2 match {
            case 1.0 => (label, "Iris-versicolor")
            case _ => (label, "Iris-virginica")
          }
        }
      }
      */
      })
      val trainErr = predictions.filter(r => r._1 != r._2).count.toDouble / points1.count
      (maxDepth, trainErr)
    })

    //    val points2 = data.map(d => {
    //      LabeledPoint(label2(d.last), Vectors.dense(d.init.map(_.toDouble)))
    //    })
    //    val model2 = DecisionTree.train(points2, Classification, Gini, maxDepth)

    errors.foreach(e => {
      println("Training Error for (" + e._1 + "," + e._2 + ")")
    })

    spark.stop()
  }

  def label1(label: String): Double = {
    label match {
      case "F" => 1
      case _ => 0
    }
  }

  def label2(label: String): Double = {
    label match {
      case "M" => 1
      case _ => 0
    }
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