package me.glorysdj.cabiria.ml.spark.mllib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author dongjie.shi
 *
 */
object KNN {

  //spark-submit --class me.glorysdj.cabiria.ml.spark.mllib.KNN --deploy-mode client --master spark://ford-267163.phx-os1.stratus.dev.ebay.com:7077 /root/cabiria/ml-spark-mllib_2.10-0.1.jar
  //spark-submit --class me.glorysdj.cabiria.ml.spark.mllib.KNN --deploy-mode cluster --master yarn /root/cabiria/ml-spark-mllib_2.10-0.1.jar
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("KNN").set("spark.executor.memory", "3g")
    val spark = new SparkContext(conf)
    val data = spark.textFile("hdfs://ford-267163.phx-os1.stratus.dev.ebay.com:8020/user/root/input/glasses/")
    val formated = data.map(_.split(",")).map(d => {
      (d.init.map(_.toDouble), d.last)
    })
    val testSet = formated.sample(false, 0.1, 0).collect
    val filterSet = spark.broadcast(testSet.toSet)
    val trainSet = formated.filter(!filterSet.value.contains(_))
    val dises = testSet.map(test => {
      val features = test._1
      val tag = test._2
      val dis = trainSet.map(train => {
        (distance(features, train._1), train._2)
      })
      (dis, tag)
    })
    val testSize = testSet.length
    List.range(0, testSize / 2).map(i => {
      val k = 2 * i + 1
      val result = dises.map(dis => {
        val tag = dis._2
        val target = dis._1.collect.sortBy(_._1).take(k).groupBy(_._2).map(kv => (kv._1, kv._2.size)).toList.sortBy(_._2).last._1
        (target, tag)
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

  def distance(x: Array[Double], y: Array[Double]) = {
    math.sqrt(List.range(0, x.length).map(i => (math.pow(x(i) - y(i), 2))).sum)
  }
}