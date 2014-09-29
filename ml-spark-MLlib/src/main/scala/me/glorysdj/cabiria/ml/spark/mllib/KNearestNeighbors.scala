/**
 *
 */
package me.glorysdj.cabiria.ml.spark.mllib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vector

/**
 * @author dongjie.shi
 *
 */
object KNearestNeighbors {

  //spark-submit --class me.glorysdj.cabiria.ml.spark.mllib.KNearestNeighbors --deploy-mode client --master spark://ford-267163.phx-os1.stratus.dev.ebay.com:7077 /root/cabiria/ml-spark-mllib_2.10-0.1.jar
  //spark-submit --class me.glorysdj.cabiria.ml.spark.mllib.KNearestNeighbors --deploy-mode cluster --master yarn /root/cabiria/ml-spark-mllib_2.10-0.1.jar
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("KNearestNeighbors").set("spark.executor.memory", "3g")
    val spark = new SparkContext(conf)
    val data = spark.textFile("hdfs://ford-267163.phx-os1.stratus.dev.ebay.com:8020/user/root/input/glasses/")
    val points = data.map(_.split(",")).map(d => {
      LabeledPoint(d.last.toDouble, Vectors.dense(d.init.map(_.toDouble)))
    })
    val testSet = points.sample(false, 0.1, 0).collect
    val filterSet = spark.broadcast(testSet.toSet)
    val trainSet = points.filter(!filterSet.value.contains(_))
    val dises = testSet.map(test => {
      val dis = trainSet.map(train => {
        (distance(test.features, train.features), train.label)
      })
      (dis, test.label)
    })
    val testSize = testSet.length
    List.range(0, testSize / 2).map(i => {
      val k = 2 * i + 1
      val result = dises.map(dis_lable => {
        val target = dis_lable._1.collect.sortBy(_._1).take(k).groupBy(_._2).map(kv => (kv._1, kv._2.size)).toList.sortBy(_._2).last._1
        (target, dis_lable._2)
      })
      val errorRate = result.filter(r => (r._1 != r._2)).size * 100.0 / testSize
      (k, result, errorRate)
    }).map(tuple => {
      //println("knn for " + tuple._1)
      //tuple._2.foreach(println)
      println("the knn (k, error rate) is about (" + "%03d".format(tuple._1) + "," + tuple._3 + "%)")
    })
    spark.stop()
  }

  def distance(v1: Vector, v2: Vector): Double = {
    math.sqrt((v1.toArray zip v2.toArray).map {
      case (elm1: Double, elm2: Double) => math.pow(elm1 - elm2, 2)
    }.sum)
  }

}