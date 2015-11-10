/**
  * project：hadoop-mr
  * file：IntPairWritable.java
  * author：zxh
  * time：2015年8月13日 下午3:06:09
  * description：
  */
package cn.com.dimensoft.hadoop.mr.entry;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * class： IntPairWritable
 * package： cn.com.dimensoft.hadoop.mr.entry
 * author：zxh
 * time： 2015年8月13日 下午3:06:09
 * description： 
 */
public class IntPairWritable implements WritableComparable<IntPairWritable>{

	private int key;
	private int value;
	
	public IntPairWritable() {
		super();
	}
	
	public IntPairWritable(int key, int value) {
		super();
		this.key = key;
		this.value = value;
	}

	public int getKey() {
		return key;
	}

	public void setKey(int key) {
		this.key = key;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + key;
		result = prime * result + value;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IntPairWritable other = (IntPairWritable) obj;
		if (key != other.key)
			return false;
		if (value != other.value)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return key + "\t" + value;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(key);
		out.writeInt(value);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.key = in.readInt();
		this.value = in.readInt();
	}

	@Override
	public int compareTo(IntPairWritable o) {
		
		if (this.key > o.key) {
			return 1;
		}else if (this.key < o.key) {
			return -1;
		}else if (this.value > o.value) {
			return 1;
		}else if (this.value < o.value) {
			return -1;
		}
		
		return 0;
	}	
	
}
