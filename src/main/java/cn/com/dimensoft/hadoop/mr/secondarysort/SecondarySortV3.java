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
import org.apache.hadoop.io.NullWritable;
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
import cn.com.dimensoft.hadoop.mr.secondarysort.partitioner.KeyPartitioner;

/**
 * class： Template
 * package： cn.com.dimensoft.hadoop.mr
 * author：zxh
 * time： 2015年8月10日 下午3:50:17
 * description：这里相对于上两个版本的二次排序又做了一点修改，并没有再进行自定义的分组了，因为的确是没有必要，
 * 将自带的数据key/value组合成新的数据类型IntPairWritable，对compareTo方法做好相关排序后就可以，
 * map输出到reduce输入之前的数据排序过程默认就是使用的map输出的key的compareTo方法，所以直接将map的
 * 输出数据类型修改为IntPairWritable，输出改成NullWritable
 */
public class SecondarySortV3 extends Configured implements Tool {

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
		
		if (args == null || args.length == 0) {
			args = new String[]{"/user/hadoop/mr/secondarysort/input", "/user/hadoop/mr/secondarysort/output"};
		}
		
		System.exit(ToolRunner.run(new SecondarySortV3(), args));
	}
	
	
	@Override
	public int run(String[] args) throws Exception {

//		get configuration
		Configuration conf = this.getConf();
		
		conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "\t");
//		create job
		Job job = Job.getInstance(conf);
		
//		set jar
		job.setJarByClass(SecondarySortV3.class);
		
//		set Mapper
		job.setMapperClass(SecondarySortMapper.class);
		
//		set inputformat
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
//		set map output key/value
		job.setMapOutputKeyClass(IntPairWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		
//		set partitioner
		job.setPartitionerClass(KeyPartitioner.class);
		
//		set group comparator
//		job.setGroupingComparatorClass(GroupASCComparator.class);
		
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
	public static class SecondarySortMapper extends Mapper<Text, Text, IntPairWritable, NullWritable>{

		
		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			
			context.write(new IntPairWritable(Integer.valueOf(key.toString()), Integer.valueOf(value.toString())), NullWritable.get());
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
	public static class SecondarySortReducer extends Reducer<IntPairWritable, NullWritable, IntPairWritable, NullWritable>{

		@Override
		protected void reduce(IntPairWritable key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			
				context.write(key, NullWritable.get());
		}
	}
	
}
