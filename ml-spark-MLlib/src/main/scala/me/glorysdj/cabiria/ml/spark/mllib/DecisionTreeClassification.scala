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
object DecisionTreeClassification {

  //spark-submit --class me.glorysdj.cabiria.ml.spark.mllib.DecisionTreeClassification --deploy-mode client --master spark://ford-267163.phx-os1.stratus.dev.ebay.com:7077 /root/cabiria/ml-spark-mllib_2.10-0.1.jar hdfs://ford-267163.phx-os1.stratus.dev.ebay.com:8020/user/root/input/data/mllib/sample_tree_data.csv
  //spark-submit --class me.glorysdj.cabiria.ml.spark.mllib.DecisionTreeClassification --deploy-mode cluster --master yarn /root/cabiria/ml-spark-mllib_2.10-0.1.jar
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: DecisionTreeClassification <in>")
      System.exit(1)
    }
    val in = args(0)

    val conf = new SparkConf().setAppName("DecisionTreeClassification").set("spark.executor.memory", "3g")
    val spark = new SparkContext(conf)
    val data = spark.textFile(in)
    val points = data.map(_.split(",")).map(d => {
      LabeledPoint(d.head.toDouble, Vectors.dense(d.tail.map(_.toDouble)))
    })
    points.cache
    // Run training algorithm to build the model
    val errors = List.range(1, 11).map(maxDepth => {
      val model = DecisionTree.train(points, Classification, Gini, maxDepth)

      // Evaluate model on training examples and compute training error
      val labelAndPreds = points.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }
      //labelAndPreds.collect.foreach(println)

      val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / points.count
      (maxDepth, trainErr)
    })
    errors.foreach(e => {
      println("Training Error for (" + e._1 + "," + e._2 + ")")
    })
    spark.stop()
  }

}