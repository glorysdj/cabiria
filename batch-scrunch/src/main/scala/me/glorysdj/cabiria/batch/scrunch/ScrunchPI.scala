/**
 *
 */
package me.glorysdj.cabiria.batch.scrunch

import org.apache.crunch.lib.Aggregate
import org.apache.crunch.scrunch.PCollection
import org.apache.crunch.scrunch.PipelineApp

/**
 * @author dongjie.shi
 *
 */
object ScrunchPI extends PipelineApp {

  // hadoop jar /root/cabiria/batch-scrunch-0.0.1-SNAPSHOT-job.jar me.glorysdj.cabiria.batch.scrunch.ScrunchPI /user/root/input/pi
  override def run(args: Array[String]) {
    if (args.length != 1) {
      System.err.println("Usage: ScrunchPI <input path>")
      System.exit(1)
    }
    val in = args(0)
    val input = read(from.textFile(in)).filter(_ != "")
    val numed = input.map(_.toInt)
    val calculated = numed.map(i => math.pow(-1, i - 1) * (1.0 / (i * 2 - 1)))
    val sumed = new PCollection(Aggregate.aggregate(calculated.native, new SumDouble()))
    val results = sumed.materialize()
    results.foreach(r => println(r * 4))
  }
}

