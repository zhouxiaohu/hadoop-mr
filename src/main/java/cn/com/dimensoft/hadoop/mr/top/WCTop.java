/**
  * project：hadoop-mr
  * file：Template.java
  * author：zxh
  * time：2015年8月10日 下午3:50:17
  * description：
  */
package cn.com.dimensoft.hadoop.mr.top;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
public class WCTop extends Configured implements Tool {

	public static final int TOP = 3;
	
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
		
		args = new String[]{"/user/hadoop/mr/wctop/input", "/user/hadoop/mr/wctop/output"};
		
		System.exit(ToolRunner.run(new WCTop(), args));
	}
	
	
	@Override
	public int run(String[] args) throws Exception {

//		get configuration
		Configuration conf = this.getConf();
		
//		create job
		Job job = Job.getInstance(conf);
		
//		set jar
		job.setJarByClass(WCTop.class);
		
//		set Mapper
		job.setMapperClass(WCTopMapper.class);
		
//		set map output key/value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
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
		job.setReducerClass(WCTopReducer.class);
		
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
	public static class WCTopMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

		public static final LongWritable COUNT = new LongWritable(1);
		
		public Text word = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
//			得到当前输入行
			String line = value.toString();
//			创建StringTokenizer
			StringTokenizer st = new StringTokenizer(line);
//			遍历st
			String item = null;
			while(st.hasMoreTokens()){
				item = st.nextToken();
//				将当前得到的单词设置到word中，作为map的输出
				word.set(item);
				context.write(word, COUNT);
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
	public static class WCTopReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

		private Map<String, Long> map = new HashMap<String, Long>();
		
		@Override
		protected void reduce(Text key, Iterable<LongWritable> counts, Context context) throws IOException, InterruptedException {

			Long sum = 0l;
			for(LongWritable currentCount : counts){
				sum += currentCount.get();
			}
			map.put(key.toString(), sum);
		}
		
		
		@Override
		protected void cleanup(Context context)	throws IOException, InterruptedException {
			
			List<Map.Entry<String, Long>> list = new ArrayList<Map.Entry<String, Long>>(map.entrySet());  
			
//			sort by value
			Collections.sort(list, new DescComparator());
			
			for(int i = 0; i < list.size(); i++){
				if (i < TOP) {
					context.write(new Text(list.get(i).getKey()), new LongWritable(list.get(i).getValue()));
				}else {
					break;
				}
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
	public static class DescComparator implements Comparator<Map.Entry<String, Long>>{

		@Override
		public int compare(Entry<String, Long> o1, Entry<String, Long> o2) {
			
			return o1.getValue() > o2.getValue() ? -1 : (o1.getValue() < o2.getValue() ? 1 : 0);
		}
		
	}
	
}
