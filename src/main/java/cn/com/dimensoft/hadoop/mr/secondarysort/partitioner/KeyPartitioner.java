/**
  * project：hadoop-mr
  * file：KeyPartitioner.java
  * author：zxh
  * time：2015年8月13日 下午4:29:30
  * description：
  */
package cn.com.dimensoft.hadoop.mr.secondarysort.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import cn.com.dimensoft.hadoop.mr.entry.IntPairWritable;

/**
 * class： KeyPartitioner
 * package： cn.com.dimensoft.hadoop.mr.secondarysort.partitioner
 * author：zxh
 * time： 2015年8月13日 下午4:29:30
 * description： 
 */
public class KeyPartitioner extends Partitioner<IntPairWritable, IntWritable> {

	@Override
	public int getPartition(IntPairWritable key, IntWritable value, int numPartitions) {
		
		return Math.abs(key.getKey() * 127) % numPartitions;
	}

}
