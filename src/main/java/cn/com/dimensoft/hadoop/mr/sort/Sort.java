/**
  * project：hadoop-mr
  * file：Template.java
  * author：zxh
  * time：2015年8月10日 下午3:50:17
  * description：
  */
package cn.com.dimensoft.hadoop.mr.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.com.dimensoft.hadoop.mr.sort.comparator.DescComparator;

/**
 * class： Sort
 * package： cn.com.dimensoft.hadoop.mr
 * author：zxh
 * time： 2015年8月10日 下午3:50:17
 * description： 
 */
public class Sort extends Configured implements Tool {

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
		
		args = new String[]{"/user/hadoop/mr/sort/input", "/user/hadoop/mr/sort/output"};
		
		System.exit(ToolRunner.run(new Sort(), args));
	}
	
	
	@Override
	public int run(String[] args) throws Exception {

//		get configuration
		Configuration conf = this.getConf();
		
//		create job
		Job job = Job.getInstance(conf);
		
//		set jar
		job.setJarByClass(Sort.class);
		
//		set Mapper
		job.setMapperClass(SortMapper.class);
		
//		set map output key/value
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		
//		set desc comparator
		job.setSortComparatorClass(DescComparator.class);
		
//		set Reducer
		job.setReducerClass(SortReducer.class);
		
//		set input path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
//		set output path
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
		
	}

	
	/**
	 * 
	 * class： SortMapper
	 * package： cn.com.dimensoft.hadoop.mr
	 * author：zxh
	 * time： 2015年8月10日 下午4:06:42
	 * description：custom logic
	 */
	public static class SortMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable>{

		private IntWritable count = new IntWritable();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			count.set(Integer.valueOf(value.toString()));
			context.write(count, NullWritable.get());
		}
		
	}
	
	/**
	 * 
	 * class： SortReducer
	 * package： cn.com.dimensoft.hadoop.mr
	 * author：zxh
	 * time： 2015年8月10日 下午4:09:10
	 * description：custom logic
	 */
	public static class SortReducer extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable>{

		@Override
		protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			
			context.write(key, NullWritable.get());
		}
		
	}
	
}
