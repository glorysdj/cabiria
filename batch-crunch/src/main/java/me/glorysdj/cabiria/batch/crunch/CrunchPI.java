/**
 * 
 */
package me.glorysdj.cabiria.batch.crunch;

import java.io.Serializable;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author dongjie.shi
 * 
 */
public class CrunchPI extends Configured implements Tool, Serializable {

	private static final long serialVersionUID = 4221778835076504240L;

	// hadoop jar /root/cabiria/batch-crunch-0.0.1-SNAPSHOT-job.jar me.glorysdj.cabiria.batch.crunch.CrunchPI /user/root/input/pi
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new CrunchPI(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("Usage: CrunchPI <input path>");
			System.exit(-1);
		}
		String in = args[0];
		Pipeline pipeline = new MRPipeline(CrunchPI.class, getConf());
		PCollection<String> input = pipeline.readTextFile(in).filter(Functions.FILTER_NULL);
		PCollection<Long> numed = input.parallelDo(Functions.PARSE_STRING_TO_LONG, Writables.longs());
		PCollection<Double> calculated = numed.parallelDo(Functions.GENERATE_CALCULATED, Writables.doubles());
		PCollection<Double> sumed = calculated.aggregate(Aggregators.SUM_DOUBLES());
		Iterable<Double> results = sumed.materialize();
		for (Double r : results) {
			System.out.println(4 * r);
		}
		PipelineResult result = pipeline.done();
		return result.succeeded() ? 0 : 1;
	}
}
