/**
  * project：hadoop-mr
  * file：Template.java
  * author：zxh
  * time：2015年8月10日 下午3:50:17
  * description：
  */
package cn.com.dimensoft.hadoop.mr.top;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

/**
 * class： Template
 * package： cn.com.dimensoft.hadoop.mr
 * author：zxh
 * time： 2015年8月10日 下午3:50:17
 * description： 
 */
public class Top extends Configured implements Tool {

	public static final int TOP = 5;
	
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
		
		args = new String[]{"/user/hadoop/mr/top/input", "/user/hadoop/mr/top/output"};
		
		System.exit(ToolRunner.run(new Top(), args));
	}
	
	
	@Override
	public int run(String[] args) throws Exception {

//		get configuration
		Configuration conf = this.getConf();
		
//		create job
		Job job = Job.getInstance(conf);
		
//		set jar
		job.setJarByClass(Top.class);
		
//		set Mapper
		job.setMapperClass(TopMapper.class);
		
//		set map output key/value
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		
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
		job.setReducerClass(TopReducer.class);
		
		job.setNumReduceTasks(1);
		
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
	public static class TopMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable>{

		public TreeSet<Long> set = new TreeSet<Long>(new DescComparator());
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			set.add(Long.parseLong(value.toString()));
			
			if (set.size() > TOP) {
				set.remove(set.last());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			
			for(Long value : set){
				context.write(new LongWritable(value), NullWritable.get());
			}
		}
	}
	
	/**
	 * 
	 * class： TopReducer
	 * package： cn.com.dimensoft.hadoop.mr
	 * author：zxh
	 * time： 2015年8月11日 下午1:51:51
	 * description：
	 */
	public static class TopReducer extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable>{

		public TreeSet<Long> set = new TreeSet<Long>(new DescComparator());

		@Override
		protected void reduce(LongWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

			set.add(key.get());
			
			if (set.size() > TOP) {
				set.remove(set.last());
			}
		}
		
		@Override
		protected void cleanup(Context context)	throws IOException, InterruptedException {
			
			for(Long value : set){
				context.write(new LongWritable(value), NullWritable.get());
			}
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
	public static class DescComparator implements Comparator<Long>{

		@Override
		public int compare(Long o1, Long o2) {
			
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
	public static class AscComparator implements Comparator<Long>{

		@Override
		public int compare(Long o1, Long o2) {
			
			if(o1 < o2){
				return -1;
			} else if(o1 > o2){
				return 1;
			}
			
			return 0;
		}
		
	}
	
}
