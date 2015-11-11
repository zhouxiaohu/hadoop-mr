package cn.com.dimensoft.hadoop.mr.distributedcache;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * class： DistributedCacheTest
 * package： cn.com.dimensoft.hadoop.mr.distributedcache
 * author：zxh
 * time： 2015年11月10日 下午2:12:50
 * description：分布式缓存，它能够自动将指定的文件分发到各个节点上，缓存到本地，供用户程序读取使用。
 */
public class DistributedCacheTest extends Configured implements Tool {

	/**
	 * 
	 * name：main
	 * author：zxh
	 * time：2015年11月10日 下午2:13:45
	 * description：
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		args = new String[] { "/user/hadoop/mr/distributedcache/input",
				"/user/hadoop/mr/distributedcache/output" };

		System.exit(ToolRunner.run(new DistributedCacheTest(), args));
	}

	@Override
	public int run(String[] args) throws Exception {

		// get configuration
		Configuration conf = this.getConf();

		// create job
		Job job = Job.getInstance(conf);

		// set jar
		job.setJarByClass(DistributedCacheTest.class);

		// set Mapper
		job.setMapperClass(MyMapper.class);


		// set Reducer
		job.setNumReduceTasks(0);

		// set DistributedCache
		job.addCacheFile(new URI(
				"/user/hadoop/mr/distributedcache/cache/dict.txt"));

		// set input path
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// set output path
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;

	}

	/**
	 * 
	 * class： MyMapper
	 * package： cn.com.dimensoft.hadoop.mr.distributedcache
	 * author：zxh
	 * time： 2015年11月10日 下午2:13:52
	 * description：
	 */
	public static class MyMapper extends
			Mapper<LongWritable, Text, IntWritable, Text> {

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			
			// 打开分布式缓存文件进行分析
			URI[] uris = context.getCacheFiles();

			FileSystem system = FileSystem.get(context.getConfiguration());

			// 简单的将缓存文件打印出来，实际场景中可以将缓存文件中的内容保存到变量里，然后在map方法中就可以使用了
			IOUtils.copy(system.open(new Path(uris[0])), System.err);

		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			super.map(key, value, context);
		}

	}

}
