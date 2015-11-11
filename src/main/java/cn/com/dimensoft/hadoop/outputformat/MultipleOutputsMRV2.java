/**
  * project：hadoop-mr
  * file：Template.java
  * author：zxh
  * time：2015年8月10日 下午3:50:17
  * description：
  */
package cn.com.dimensoft.hadoop.outputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * class： Template
 * package： cn.com.dimensoft.hadoop.mr
 * author：zxh
 * time： 2015年8月10日 下午3:50:17
 * description： 
 */
public class MultipleOutputsMRV2 extends Configured implements Tool {

	/**
	 * 
	 * name：main
	 * author：zxh
	 * time：2015年8月10日 下午4:06:08
	 * description：
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {

		args = new String[] { "/user/hadoop/mr/MultipleOutputs/input",
				"/user/hadoop/mr/MultipleOutputs/output2" };

		System.exit(ToolRunner.run(new MultipleOutputsMRV2(), args));
	}

	@Override
	public int run(String[] args) throws Exception {

		// get configuration
		Configuration conf = this.getConf();

		// create job
		Job job = Job.getInstance(conf);

		// set jar
		job.setJarByClass(MultipleOutputsMRV2.class);

		// set Mapper
		job.setMapperClass(MutltiMapper.class);

		// set map output key/value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		// MultipleOutputs
		MultipleOutputs.addNamedOutput(job, "box", TextOutputFormat.class,
				Text.class, NullWritable.class);

		MultipleOutputs.setCountersEnabled(job, true);

		job.setNumReduceTasks(0);

		// set input path
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// set output path
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;

	}

	/**
	 * 
	 * class： MutltiMapper
	 * package： cn.com.dimensoft.hadoop.mr
	 * author：zxh
	 * time： 2015年8月10日 下午4:06:42
	 * description：custom logic
	 */
	public static class MutltiMapper extends
			Mapper<LongWritable, Text, Text, NullWritable> {

		private MultipleOutputs<Text, NullWritable> mos;

		@Override
		public void setup(Context context) {
			mos = new MultipleOutputs<Text, NullWritable>(context);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			if (value.toString().matches("[a-z]+")) {
				mos.write("box", value, NullWritable.get(), "alphabet");
			} else {
				mos.write("box", value, NullWritable.get(), "number");
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

			mos.close();
		}

	}
}
