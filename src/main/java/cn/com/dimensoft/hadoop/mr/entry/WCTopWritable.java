/**
  * project：hadoop-mr
  * file：WCTopWritable.java
  * author：zxh
  * time：2015年8月11日 下午2:43:45
  * description：
  */
package cn.com.dimensoft.hadoop.mr.entry;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.io.WritableComparable;

/**
 * class： WCTopWritable
 * package： cn.com.dimensoft.hadoop.mr.entry
 * author：zxh
 * time： 2015年8月11日 下午2:43:45
 * description： 
 */
public class WCTopWritable implements WritableComparable<WCTopWritable> {

	private Long count;
	
	private String word;
	
	
	public WCTopWritable() {
		super();
	}

	public WCTopWritable(Long count, String word) {
		super();
		this.count = count;
		this.word = word;
	}

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(count);
		out.writeUTF(word);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.count = in.readLong();
		this.word = in.readUTF();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((count == null) ? 0 : count.hashCode());
		result = prime * result + ((word == null) ? 0 : word.hashCode());
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
		WCTopWritable other = (WCTopWritable) obj;
		if (count == null) {
			if (other.count != null)
				return false;
		} else if (!count.equals(other.count))
			return false;
		if (word == null) {
			if (other.word != null)
				return false;
		} else if (!word.equals(other.word))
			return false;
		return true;
	}

	@Override
	public int compareTo(WCTopWritable o) {
		
		return count < o.getCount() ? 1 : (count > o.getCount() ? -1 : 0);
	}

	@Override
	public String toString() {
		return count + "\t" + word;
	}
	
	public static void main(String[] args) {
		
		TreeSet<WCTopWritable> set = new TreeSet<WCTopWritable>();
		
		WCTopWritable o1 = new WCTopWritable(1l, "any");
		WCTopWritable o2 = new WCTopWritable(1l, "hadoop");
		
		set.add(o1);
		set.add(o2);
		
		System.out.println(set);
	}

}
