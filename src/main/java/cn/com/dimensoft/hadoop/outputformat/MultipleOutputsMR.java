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
public class MultipleOutputsMR extends Configured implements Tool {

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
		
		args = new String[]{"/user/hadoop/mr/multi/input", "/user/hadoop/mr/multi/output"};
		
		System.exit(ToolRunner.run(new MultipleOutputsMR(), args));
	}
	
	
	@Override
	public int run(String[] args) throws Exception {

//		get configuration
		Configuration conf = this.getConf();
		
//		create job
		Job job = Job.getInstance(conf);
		
//		set jar
		job.setJarByClass(MultipleOutputsMR.class);
		
//		set Mapper
		job.setMapperClass(MutltiMapper.class);
		
//		set map output key/value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
//		MultipleOutputs
		MultipleOutputs.addNamedOutput(job, "alphabet", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "number", TextOutputFormat.class, Text.class, NullWritable.class);
		
		/**		shuffle start	**/
//		set patition
//		job.setPartitionerClass(HashPartitioner.class);

//		set sort comparator
//		job.setSortComparatorClass(IntWritable.Comparator.class);
		
//		set combiner
//		job.setCombinerClass(MyReducer.class);
		
//		set group comparator
//		job.setGroupingComparatorClass(IntWritable.Comparator.class);
		/**		shuffle end	**/
		
//		set Reducer
//		job.setReducerClass(MyReducer.class);
		
		job.setNumReduceTasks(0);
		
//		set input path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
//		set output path
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
	public static class MutltiMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

		private MultipleOutputs<Text, NullWritable> mos;
		
		@Override
		public void setup(Context context) {
			 mos = new MultipleOutputs<Text, NullWritable>(context);
		 }
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			if (value.toString().matches("[a-z]+")) {
				mos.write("alphabet", value, NullWritable.get());
			} else {
				mos.write("number", value, NullWritable.get());
			}
		}
		
	}
}
