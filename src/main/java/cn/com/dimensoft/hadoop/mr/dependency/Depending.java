/**
  * project：hadoop-mr
  * file：Dependening.java
  * author：zxh
  * time：2015年8月24日 下午2:14:46
  * description：
  */
package cn.com.dimensoft.hadoop.mr.dependency;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * class： Depending
 * package： cn.com.dimensoft.hadoop.mr.dependency
 * author：zxh
 * time： 2015年8月24日 下午2:14:46
 * description： 
 */
public class Depending extends Configured implements Tool {

	/**
	 * name：main
	 * author：zxh
	 * time：2015年8月24日 下午2:14:46
	 * description：
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {

		if (args == null || args.length == 0) {
			args = new String[] { "/user/hadoop/mr/dependency/input",
					"/user/hadoop/mr/dependency/middle",
					"/user/hadoop/mr/dependency/output" };
		}

		System.exit(ToolRunner.run(new Depending(), args));
	}

	@Override
	public int run(String[] args) throws Exception {

		// configuration
		Configuration conf = this.getConf();

		// create dep-wc job
		Job wc = Job.getInstance(conf, "dep-wc");
		wc.setJarByClass(Depending.class);

		// set map class
		wc.setMapperClass(WordCountMapper.class);
		// set reduce class
		wc.setReducerClass(WordCountReducer.class);
		// set map output key
		wc.setMapOutputKeyClass(Text.class);
		// set map output value
		wc.setMapOutputValueClass(IntWritable.class);

		// set reduce number
		wc.setNumReduceTasks(1);

		// set input path
		FileInputFormat.addInputPath(wc, new Path(args[0]));
		// set output path
		FileOutputFormat.setOutputPath(wc, new Path(args[1]));

		// create controlled
		ControlledJob wcControlledJob = new ControlledJob(conf);
		wcControlledJob.setJob(wc);

		// ======================================================================


		// create dep-sort job
		Job sort = Job.getInstance(conf, "dep-sort");
		sort.setJarByClass(Depending.class);

		// set inputformat class
		sort.setInputFormatClass(KeyValueTextInputFormat.class);
		
		// set map class
		sort.setMapperClass(SortMapper.class);
		// set map output key
		sort.setOutputKeyClass(Text.class);
		// set map output value
		sort.setOutputValueClass(IntWritable.class);

		// set reduce number
		sort.setNumReduceTasks(0);

		// set input path
		FileInputFormat.addInputPath(sort, new Path(args[1]));
		// set output path
		FileOutputFormat.setOutputPath(sort, new Path(args[2]));

		
		// create controlled
		ControlledJob sortControlledJob = new ControlledJob(conf);
		sortControlledJob.setJob(sort);

		// add dependency
		sortControlledJob.addDependingJob(wcControlledJob);

		// create control
		JobControl jc = new JobControl("control");
		jc.addJob(wcControlledJob);
		jc.addJob(sortControlledJob);

		// create thread
		Thread jobControlThread = new Thread(jc);
		// start thread
		jobControlThread.start();

		while (!jc.allFinished()) {
			Thread.sleep(500);
		}

		jc.stop();
		
		return 0;
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

	/**
	 * 
	 * class： SortMapper
	 * package： cn.com.dimensoft.hadoop.mr
	 * author：zxh
	 * time： 2015年8月10日 下午4:06:42
	 * description：custom logic
	 */
	public static class SortMapper extends
			Mapper<Text, Text, Text, IntWritable> {

		private HashMap<String, Integer> map = new HashMap<String, Integer>();

		private Text word = new Text();
		private IntWritable count = new IntWritable();

		@Override
		protected void map(Text word, Text count, Context context)
				throws IOException, InterruptedException {

			map.put(word.toString(), Integer.parseInt(count.toString()));
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

			// sort the map

			List<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>(
					map.entrySet());
			Collections.sort(list, new DescComparator());

			// write
			for (Entry<String, Integer> entry : list) {
				word.set(entry.getKey());
				count.set(entry.getValue());
				context.write(word, count);
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
	public static class DescComparator implements
			Comparator<Entry<String, Integer>> {

		@Override
		public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {

			return o1.getValue() > o2.getValue() ? -1 : (o1.getValue() < o2
					.getValue() ? 1 : 0);
		}

	}

}
