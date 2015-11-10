/**
  * project：hadoop-mr
  * file：GroupASCComparator.java
  * author：zxh
  * time：2015年8月13日 下午3:20:23
  * description：
  */
package cn.com.dimensoft.hadoop.mr.secondarysort.comparator;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

import cn.com.dimensoft.hadoop.mr.entry.IntPairWritable;

/**
 * class： GroupASCComparator
 * package： cn.com.dimensoft.hadoop.mr.secondarysort
 * author：zxh
 * time： 2015年8月13日 下午3:20:23
 * description： 
 */
public class GroupASCComparator implements RawComparator<IntPairWritable> {

	@Override
	public int compare(IntPairWritable o1, IntPairWritable o2) {
		
		return o1.getKey() > o2.getKey() ? 1 : (o1.getKey() < o2.getKey() ? -1 : 0);
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		
		return WritableComparator.compareBytes(b1, s1, 4, b2, s2, 4);
	}

}
