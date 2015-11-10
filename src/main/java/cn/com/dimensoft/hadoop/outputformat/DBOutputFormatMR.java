/**
  * project：hadoop-mr
  * file：Template.java
  * author：zxh
  * time：2015年8月10日 下午3:50:17
  * description：
  */
package cn.com.dimensoft.hadoop.outputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.com.dimensoft.hadoop.mr.entry.User;

/**
 * class： Template
 * package： cn.com.dimensoft.hadoop.mr
 * author：zxh
 * time： 2015年8月10日 下午3:50:17
 * description： 
 */
public class DBOutputFormatMR extends Configured implements Tool {

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
		
		args = new String[]{"/user/hadoop/mr/db/input/"};
		
		System.exit(ToolRunner.run(new DBOutputFormatMR(), args));
	}
	
	
	@Override
	public int run(String[] args) throws Exception {

//		get configuration
		Configuration conf = this.getConf();
		
//		set jdbc
		conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "com.mysql.jdbc.Driver");
		conf.set(DBConfiguration.URL_PROPERTY, "jdbc:mysql://localhost:3306/test");
		conf.set(DBConfiguration.USERNAME_PROPERTY, "root");
		conf.set(DBConfiguration.PASSWORD_PROPERTY, "pass123");
		
//		create job
		Job job = Job.getInstance(conf);
		
//		set jar
		job.setJarByClass(DBOutputFormatMR.class);
		
//		set Mapper
		job.setMapperClass(DBOutputFormatMapper.class);
		
		job.setOutputFormatClass(DBOutputFormat.class);
		
		DBOutputFormat.setOutput(job, "my_user", "id", "account", "passwd");
		
		job.setNumReduceTasks(0);
		
//		set input path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
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
	public static class DBOutputFormatMapper extends Mapper<LongWritable, Text, User, NullWritable>{

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] array = value.toString().split(",");
			
			User user = new User(Integer.parseInt(array[0]), array[1], array[2]);
			
			context.write(user, NullWritable.get());
		}
		
	}
	
}
