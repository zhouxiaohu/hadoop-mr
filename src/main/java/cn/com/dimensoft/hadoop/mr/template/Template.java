/**
  * project：hadoop-mr
  * file：Template.java
  * author：zxh
  * time：2015年8月10日 下午3:50:17
  * description：
  */
package cn.com.dimensoft.hadoop.mr.template;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * class： Template
 * package： cn.com.dimensoft.hadoop.mr
 * author：zxh
 * time： 2015年8月10日 下午3:50:17
 * description： 
 */
public class Template extends Configured implements Tool {

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
		
		System.exit(ToolRunner.run(new Template(), args));
	}
	
	
	@Override
	public int run(String[] args) throws Exception {

//		get configuration
		Configuration conf = this.getConf();
		
//		create job
		Job job = Job.getInstance(conf);
		
//		set jar
		job.setJarByClass(Template.class);
		
//		set Mapper
		job.setMapperClass(MyMapper.class);
		
//		set map output key/value
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
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
		job.setReducerClass(MyReducer.class);
		
//		set input path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
//		set output path
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
		
	}

	
	/**
	 * 
	 * class： MyMapper
	 * package： cn.com.dimensoft.hadoop.mr
	 * author：zxh
	 * time： 2015年8月10日 下午4:06:42
	 * description：custom logic
	 */
	public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			super.map(key, value, context);
		}
		
	}
	
	/**
	 * 
	 * class： MyReducer
	 * package： cn.com.dimensoft.hadoop.mr
	 * author：zxh
	 * time： 2015年8月10日 下午4:09:10
	 * description：custom logic
	 */
	public static class MyReducer extends Reducer<IntWritable, Text, IntWritable, Text>{

		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			super.reduce(key, values, context);
		}
		
	}
	
}
