/**
  * project：hadoop-mr
  * file：WordCount.java
  * author：zxh
  * time：2015年7月14日 下午4:50:16
  * description：
  */
package cn.com.dimensoft.hadoop.mr.wc;

import java.io.IOException;
import java.util.StringTokenizer;

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
 * class： WordCount
 * package： cn.com.dimensoft.hadoop.mr
 * author：zxh
 * time： 2015年7月14日 下午4:50:16
 * description： 
 */
public class WordCount extends Configured implements Tool {

	enum BigLineCounter {
		COUNTER
	}

	/**
	 * class： WordCountMapper
	 * package： cn.com.dimensoft.hadoop.mr
	 * author：zxh
	 * time： 2015年7月14日 下午4:51:16
	 * description：
	 */
	public static class WordCountMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		public static final IntWritable COUNT = new IntWritable(1);

		public Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// 得到当前输入行
			String line = value.toString();

			// 使用自定义的计数器统计行长度大于11的
			if (line.length() > 11) {
				context.getCounter(BigLineCounter.COUNTER).increment(1);
			}

			// 创建StringTokenizer
			StringTokenizer st = new StringTokenizer(line);
			// 遍历st
			String item = null;
			while (st.hasMoreTokens()) {
				item = st.nextToken();
				// 将当前得到的单词设置到word中，作为map的输出
				word.set(item);
				context.write(word, COUNT);
			}
		}
	}

	/**
	 * class： WordCountReducer
	 * package： cn.com.dimensoft.hadoop.mr
	 * author：zxh
	 * time： 2015年7月14日 下午4:52:01
	 * description：
	 */
	public static class WordCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public IntWritable count = new IntWritable();

		public void reduce(Text word, Iterable<IntWritable> list,
				Context context) throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable currentCount : list) {
				sum += currentCount.get();
			}

			count.set(sum);
			context.write(word, count);
		}
	}

	public int run(String[] args) throws Exception {

		// 创建configuration对象
		Configuration conf = this.getConf();

		// 创建job
		Job job = Job.getInstance(conf, "my_wordcount");
		// 设置驱动程序所在的class
		job.setJarByClass(WordCount.class);

		// 设置map
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		/**		配置shuffle start    **/
		// 分区
		// job.setPartitionerClass();

		// 排序
		// job.setSortComparatorClass();

		// combiner
		// job.setCombinerClass();
		// job.setCombinerKeyGroupingComparatorClass();

		// 压缩(需要集群支持snappy压缩)
		// conf.set("mapreduce.map.output.compress", "true");
		// conf.set("mapreduce.map.output.compress.codec",
		// "org.apache.hadoop.io.compress.SnappyCodec");

		// 分组
		// job.setGroupingComparatorClass();
		/**		配置shuffle end    **/

		// 设置reduce
		job.setReducerClass(WordCountReducer.class);

		// 输入文件路径
		FileInputFormat.addInputPath(job, new Path("/user/hadoop/mr/wc/input"));
		// 作业结果输出路径
		FileOutputFormat.setOutputPath(job, new Path(
				"/user/hadoop/mr/wc/output"));

		// 执行job
		job.waitForCompletion(true);
		return 0;

	}

	public static void main(String[] args) throws Exception {

		System.exit(ToolRunner.run(new WordCount(), args));
	}

}
