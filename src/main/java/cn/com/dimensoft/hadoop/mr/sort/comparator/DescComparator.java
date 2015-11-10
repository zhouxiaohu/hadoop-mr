/**
  * project：hadoop-mr
  * file：DescComparator.java
  * author：zxh
  * time：2015年8月13日 下午2:51:01
  * description：
  */
package cn.com.dimensoft.hadoop.mr.sort.comparator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;

/**
 * class： DescComparator
 * package： cn.com.dimensoft.hadoop.mr.sort.comparator
 * author：zxh
 * time： 2015年8月13日 下午2:51:01
 * description： 
 */
public class DescComparator implements RawComparator<IntWritable> {

	@Override
	public int compare(IntWritable o1, IntWritable o2) {
		int thisValue = o1.get();
		int thatValue = o2.get();
		return (thisValue < thatValue ? 1 : (thisValue == thatValue ? 0 : -1));
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

		int thisValue = readInt(b1, s1);
		int thatValue = readInt(b2, s2);
		return (thisValue < thatValue ? 1 : (thisValue == thatValue ? 0 : -1));
	}

	/** Parse an integer from a byte array. */
	public int readInt(byte[] bytes, int start) {
		return (((bytes[start] & 0xff) << 24)
				+ ((bytes[start + 1] & 0xff) << 16)
				+ ((bytes[start + 2] & 0xff) << 8) + ((bytes[start + 3] & 0xff)));

	}
}
