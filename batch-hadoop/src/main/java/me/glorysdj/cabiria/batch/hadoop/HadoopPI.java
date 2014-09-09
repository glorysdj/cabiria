/**
 * 
 */
package me.glorysdj.cabiria.batch.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author dongjie.shi
 * 
 */
public class HadoopPI {
	//hadoop jar /root/cabiria/batch-hadoop-0.0.1-SNAPSHOT.jar me.glorysdj.cabiria.batch.hadoop.HadoopPI /user/root/input/pi /user/root/output/pi
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: HadoopPI <input path> <output path>");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "HadoopPI");
		job.setJarByClass(HadoopPI.class);
		job.setMapperClass(HadoopPIMapper.class);
		job.setCombinerClass(HadoopPIReducer.class);
		job.setReducerClass(HadoopPIReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

class HadoopPIMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
			Double i = Double.parseDouble(value.toString());
			Double out = 4 * Math.pow(-1, i - 1) * (1.0 / (i * 2 - 1));
			context.write(new Text("PI"), new DoubleWritable(out));
		} catch (Exception e) {
		}
	}
}

class HadoopPIReducer extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		Double sum = 0.0;
		for (DoubleWritable value : values) {
			sum += value.get();
		}
		context.write(new Text("PI"), new DoubleWritable(sum));
	}
}