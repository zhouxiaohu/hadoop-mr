/**
  * project：hadoop-mr
  * file：Template.java
  * author：zxh
  * time：2015年8月10日 下午3:50:17
  * description：
  */
package cn.com.dimensoft.hadoop.mr.secondarysort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.com.dimensoft.hadoop.mr.entry.IntPairWritable;
import cn.com.dimensoft.hadoop.mr.secondarysort.comparator.GroupASCComparator;
import cn.com.dimensoft.hadoop.mr.secondarysort.partitioner.KeyPartitioner;

/**
 * class： Template
 * package： cn.com.dimensoft.hadoop.mr
 * author：zxh
 * time： 2015年8月10日 下午3:50:17
 * description： 
 */
public class SecondarySort extends Configured implements Tool {

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
		
		args = new String[]{"/user/hadoop/mr/secondarysort/input", "/user/hadoop/mr/secondarysort/output"};
		System.exit(ToolRunner.run(new SecondarySort(), args));
	}
	
	
	@Override
	public int run(String[] args) throws Exception {

//		get configuration
		Configuration conf = this.getConf();
		
		conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "\t");
//		create job
		Job job = Job.getInstance(conf);
		
//		set jar
		job.setJarByClass(SecondarySort.class);
		
//		set Mapper
		job.setMapperClass(SecondarySortMapper.class);
		
//		set inputformat
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
//		set map output key/value
		job.setMapOutputKeyClass(IntPairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
//		set partitioner
		job.setPartitionerClass(KeyPartitioner.class);
		
//		set group comparator
		job.setGroupingComparatorClass(GroupASCComparator.class);
		
//		set Reducer
		job.setReducerClass(SecondarySortReducer.class);
		
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
	public static class SecondarySortMapper extends Mapper<Text, Text, IntPairWritable, IntWritable>{

		private IntWritable secondary = new IntWritable();
		
		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			
			secondary.set(Integer.parseInt(value.toString()));
			
			context.write(new IntPairWritable(Integer.valueOf(key.toString()), Integer.valueOf(value.toString())), secondary);
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
	public static class SecondarySortReducer extends Reducer<IntPairWritable, IntWritable, IntWritable, IntWritable>{

		@Override
		protected void reduce(IntPairWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			for(IntWritable value : values){
				context.write(new IntWritable(key.getKey()), value);
			}
		}
	}
	
}
