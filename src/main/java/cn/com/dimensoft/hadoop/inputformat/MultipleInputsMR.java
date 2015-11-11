/**
  * project：hadoop-mr
  * file：Template.java
  * author：zxh
  * time：2015年8月10日 下午3:50:17
  * description：
  */
package cn.com.dimensoft.hadoop.inputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * class： MultipleInputsMR
 * package： cn.com.dimensoft.hadoop.mr
 * author：zxh
 * time： 2015年8月10日 下午3:50:17
 * description： 同时包含两个map
 * userMapper处理user.txt数据
 * depMapper处理dep.txt数据
 * 
 * user.txt(id，姓名，地址)
 * 1,zhangsan,nanjing
 * 2,lisi,shanghai
 * 
 * dep.txt(主键，部门，user表外键)
 * 1,develop,1
 * 2,test,2
 * 
 * 
 * 最终结果要求
 * 1,zhangsan,nanjing,develop
 * 2,lisi,shanghai,test
 */
public class MultipleInputsMR extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		args = new String[] { //
				"/user/hadoop/mr/MultipleInputs/user.txt",//
				"/user/hadoop/mr/MultipleInputs/dep.txt",//
				"/user/hadoop/mr/MultipleInputs/result" };

		System.exit(ToolRunner.run(new MultipleInputsMR(), args));
	}

	@Override
	public int run(String[] args) throws Exception {

		// get configuration
		Configuration conf = this.getConf();

		// create job
		Job job = Job.getInstance(conf);

		// set jar
		job.setJarByClass(MultipleInputsMR.class);

		// set mapper
		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class, UserMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class, DepMapper.class);

		// set map output key/value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// set job output key/value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// set Reducer
		job.setReducerClass(JoinReducer.class);

		// set output path
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		return job.waitForCompletion(true) ? 0 : 1;

	}

	/**
	 * 
	 * class： UserMapper
	 * package： cn.com.dimensoft.hadoop.inputformat
	 * author：zxh
	 * time： 2015年11月11日 上午9:48:10
	 * description：处理user.txt，将id主键作为map函数输出key，其他字段作为输出value，不同字段使用逗号拼接
	 * 数据形式为：1,zhangsan,nanjing
	 */
	public static class UserMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text id = new Text();
		private Text value = new Text();

		public void map(LongWritable key, Text line, Context context)
				throws IOException, InterruptedException {

			id.set(line.toString().substring(0, line.toString().indexOf(",")));
			value.set(line.toString().substring(
					line.toString().indexOf(",") + 1));

			context.write(id, value);
		}
	}

	/**
	 * 
	 * class： DepMapper
	 * package： cn.com.dimensoft.hadoop.inputformat
	 * author：zxh
	 * time： 2015年11月11日 上午9:48:14
	 * description：处理dep.txt，将外键id作为map输出key，部门字段作为输出value
	 * 数据形式为：1,develop,1
	 */
	public static class DepMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text id = new Text();
		private Text value = new Text();

		public void map(LongWritable key, Text line, Context context)
				throws IOException, InterruptedException {

			id.set(line.toString().substring(
					line.toString().lastIndexOf(",") + 1));
			value.set(line.toString().substring(
					line.toString().indexOf(",") + 1,
					line.toString().lastIndexOf(",")));

			context.write(id, value);
		}
	}

	/**
	 * 
	 * class： JoinReducer
	 * package： cn.com.dimensoft.hadoop.inputformat
	 * author：zxh
	 * time： 2015年11月11日 上午9:48:17
	 * description：最终结果要求
	 * 
	 * 1,zhangsan,nanjing,develop
	 * 2,lisi,shanghai,test
	 */
	public static class JoinReducer extends
			Reducer<Text, Text, Text, NullWritable> {

		private Text result = new Text();

		@Override
		protected void reduce(Text id, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			StringBuilder builder = new StringBuilder(id.toString());
			String department = null;
			for (Text value : values) {
				if (value.toString().contains(",")) {
					builder.append(",").append(value.toString());
				}else {
					department = value.toString();
				}
			}

			result.set(builder.append(",").append(department).toString());
			context.write(result, NullWritable.get());
		}

	}

}
