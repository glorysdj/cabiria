package me.glorysdj.cabiria.batch.crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.FilterFn;

/**
 * @author dongjie.shi
 * 
 */
public class Functions {
	public static FilterFn<String> FILTER_NULL = new FilterFn<String>() {
		private static final long serialVersionUID = 4987828972432676024L;

		@Override
		public boolean accept(String input) {
			return !input.equals("");
		}
	};

	public static DoFn<String, Long> PARSE_STRING_TO_LONG = new DoFn<String, Long>() {
		private static final long serialVersionUID = -8456116837167264379L;

		@Override
		public void process(String in, Emitter<Long> emitter) {
			emitter.emit(Long.parseLong(in));
		}
	};

	public static DoFn<Long, Long> GENERATE_NUMED = new DoFn<Long, Long>() {
		private static final long serialVersionUID = 1099592088538157880L;

		@Override
		public void process(Long in, Emitter<Long> emitter) {
			for (long i = 1; i <= in; i++) {
				emitter.emit(i);
			}
		}
	};

	public static DoFn<Long, Double> GENERATE_CALCULATED = new DoFn<Long, Double>() {
		private static final long serialVersionUID = -4846125129955923028L;

		@Override
		public void process(Long in, Emitter<Double> emitter) {
			double out = Math.pow(-1, in - 1) * (1.0 / (in * 2 - 1));
			emitter.emit(out);
		}
	};

}
