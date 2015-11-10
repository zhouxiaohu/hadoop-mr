/**
  * project：hadoop-mr
  * file：Template.java
  * author：zxh
  * time：2015年8月10日 下午3:50:17
  * description：
  */
package cn.com.dimensoft.hadoop.mr.secondarysort;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;

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

/**
 * class： Template
 * package： cn.com.dimensoft.hadoop.mr
 * author：zxh
 * time： 2015年8月10日 下午3:50:17
 * description： 
 */
public class SecondarySortV2 extends Configured implements Tool {

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
		
		args = new String[]{"/user/hadoop/mr/secondarysort/input", "/user/hadoop/mr/secondarysort/output2"};
		System.exit(ToolRunner.run(new SecondarySortV2(), args));
	}
	
	
	@Override
	public int run(String[] args) throws Exception {

//		get configuration
		Configuration conf = this.getConf();
		
		conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "\t");
//		create job
		Job job = Job.getInstance(conf);
		
//		set jar
		job.setJarByClass(SecondarySortV2.class);
		
//		set Mapper
		job.setMapperClass(SecondarySortMapper.class);
		
//		set inputformat
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
//		set map output key/value
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		
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
	 * class： SecondarySortMapper
	 * package： cn.com.dimensoft.hadoop.mr
	 * author：zxh
	 * time： 2015年8月10日 下午4:06:42
	 * description：custom logic
	 */
	public static class SecondarySortMapper extends Mapper<Text, Text, IntWritable, IntWritable>{

		private IntWritable left = new IntWritable();
		private IntWritable right = new IntWritable();
		
		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			
			
			left.set(Integer.parseInt(key.toString()));
			right.set(Integer.parseInt(value.toString()));
			
			context.write(left, right);
		}
		
	}
	
	/**
	 * 
	 * class： SecondarySortReducer
	 * package： cn.com.dimensoft.hadoop.mr
	 * author：zxh
	 * time： 2015年8月10日 下午4:09:10
	 * description：custom logic
	 */
	public static class SecondarySortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{

		private TreeSet<Integer> vs = new TreeSet<Integer>(new AscComparator());
		
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
//			sort values
			for(IntWritable value : values){
				vs.add(value.get());
			}
			
			for(Integer v : vs){
				context.write(key, new IntWritable(v));
			}
			
			vs.clear();
		}
	}
	
	/**
	 * 
	 * class： DescComparator
	 * package： cn.com.dimensoft.hadoop.mr
	 * author：zxh
	 * time： 2015年8月11日 上午10:44:47
	 * description：降序比较器
	 */
	public static class DescComparator implements Comparator<Integer>{

		@Override
		public int compare(Integer o1, Integer o2) {
			
			if(o1 < o2){
				return 1;
			} else if(o1 > o2){
				return -1;
			}
			
			return 0;
		}
		
	}
	
	/**
	 * 
	 * class： DescComparator
	 * package： cn.com.dimensoft.hadoop.mr
	 * author：zxh
	 * time： 2015年8月11日 上午10:44:47
	 * description：升序比较器
	 */
	public static class AscComparator implements Comparator<Integer>{

		@Override
		public int compare(Integer o1, Integer o2) {
			
			if(o1 < o2){
				return -1;
			} else if(o1 > o2){
				return 1;
			}
			
			return 0;
		}
		
	}
}
