/**
 *
 */
package me.glorysdj.cabiria.batch.scrunch

import org.apache.crunch.fn.Aggregators.SimpleAggregator
import com.google.common.collect.ImmutableList

/**
 * @author dongjie.shi
 *
 */
class SumDouble extends SimpleAggregator[Double] {
  private[this] var sum = 0.0
  override def reset = {
    sum = 0.0
  }
  override def update(next: Double) = {
    sum += next
  }
  override def results = {
    ImmutableList.of(sum)
  }
}