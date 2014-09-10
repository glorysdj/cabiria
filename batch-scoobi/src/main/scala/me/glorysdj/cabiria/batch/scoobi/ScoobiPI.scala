/**
 *
 */
package me.glorysdj.cabiria.batch.scoobi

import com.nicta.scoobi.Scoobi._
import Reduction._

/**
 * @author dongjie.shi
 *
 */
object ScoobiPI extends ScoobiApp {
  //hadoop jar /root/cabiria/batch-scoobi-0.0.1-SNAPSHOT-job.jar me.glorysdj.cabiria.batch.scoobi.ScoobiPI hdfs://ford-267163.phx-os1.stratus.dev.ebay.com:8020/user/root/input/pi
  def run() {
    if (args.length != 1) {
      System.err.println("Usage: SparkPI <in>")
      System.exit(1)
    }
    val in = args(0)

    val numed = fromTextFile(in).filter(_ != "").map(_.toInt)
    val calculated = numed.map(i => math.pow(-1, i - 1) * (1.0 / (i * 2 - 1)))
    val sumed = calculated.reduce(Sum.double).run
    
    println("PI is roughly " + 4.0 * sumed)
  }

}