/**
  * project：hadoop-mr
  * file：ConfigurationPrinter.java
  * time：2015年5月14日 上午9:11:14
  * description：
  */
package cn.com.dimensoft.hadoop.mr.printer;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * class： ConfigurationPrinter
 * package： cn.com.dimensoft.hadoop.mr
 * time： 2015年5月14日 上午9:11:14
 * description： 
 */
public class ConfigurationPrinter extends Configured implements Tool {

	
	static{
		Configuration.addDefaultResource("core-site.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
		Configuration.addDefaultResource("mapred-site.xml");
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {

		Configuration configuration = getConf();
		for(Entry<String, String> entry : configuration){
			System.out.println(entry.getKey() + "\t" + entry.getValue());
		}
		return 0;
	}

	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ConfigurationPrinter(), args);
	}
}
