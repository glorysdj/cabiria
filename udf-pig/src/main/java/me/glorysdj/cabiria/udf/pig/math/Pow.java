/**
 * 
 */
package me.glorysdj.cabiria.udf.pig.math;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * @author dongjie.shi
 * 
 */
public class Pow extends EvalFunc<Double> {

	@Override
	public Double exec(Tuple tuple) throws IOException {
		Double p = (Double) tuple.get(0);
		Double v = (Double) tuple.get(1);
		return Math.pow(p, v);
	}

}
