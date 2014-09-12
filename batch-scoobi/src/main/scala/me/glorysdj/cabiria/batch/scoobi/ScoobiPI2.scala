/**
 *
 */
package me.glorysdj.cabiria.batch.scoobi

import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.Scoobi.Reduction._

/**
 * @author dongjie.shi
 *
 */
object ScoobiPI2 extends ScoobiApp {

  //hadoop jar /root/cabiria/batch-scoobi-0.0.1-SNAPSHOT-job.jar me.glorysdj.cabiria.batch.scoobi.ScoobiPI2 n
  def run() {
    if (args.length != 1) {
      System.err.println("Usage: ScoobiPI2 <n>")
      System.exit(1)
    }
    val n = args(0).toInt

    val begin = System.currentTimeMillis()
    val m = 10000000
    val slices = List.range(0, n / m)
    val numed = slices.map(i => {
      val l = List.range(1, m + 1).map(j => {
        i * m + j
      })
      DList(l)
    })
    val calculated = numed.map(dl => {
      dl.mapFlatten(l => {
        l.map(i => math.pow(-1, i - 1) * (1.0 / (i * 2 - 1)))
      })
    })
    val sumed = calculated.map(_.reduce(Sum.double).run).sum

    val scoobied = System.currentTimeMillis()
    println("PI is roughly " + 4.0 * sumed)
    println("=== scoobi phase cost " + (scoobied - begin) + "ms")
  }

}