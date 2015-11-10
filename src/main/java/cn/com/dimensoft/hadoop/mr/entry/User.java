/**
  * project：hadoop-mr
  * file：User.java
  * author：zxh
  * time：2015年8月12日 下午3:13:39
  * description：
  */
package cn.com.dimensoft.hadoop.mr.entry;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 * class： User
 * package： cn.com.dimensoft.hadoop.mr.entry
 * author：zxh
 * time： 2015年8月12日 下午3:13:39
 * description： 
 */
public class User implements WritableComparable<User>, DBWritable {

	private int id;
	private String account;
	private String passwd;
	
	public User() {
	}

	public User(int id, String account, String passwd) {
		super();
		this.id = id;
		this.account = account;
		this.passwd = passwd;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getAccount() {
		return account;
	}

	public void setAccount(String account) {
		this.account = account;
	}

	public String getPasswd() {
		return passwd;
	}

	public void setPasswd(String passwd) {
		this.passwd = passwd;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.lib.db.DBWritable#write(java.sql.PreparedStatement)
	 */
	@Override
	public void write(PreparedStatement statement) throws SQLException {
		int index = 1;  
        statement.setInt(index++, this.getId());  
        statement.setString(index++, this.getAccount());  
        statement.setString(index, this.getPasswd());  
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.lib.db.DBWritable#readFields(java.sql.ResultSet)
	 */
	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
	}

	@Override
	public void write(DataOutput out) throws IOException {
	}

	@Override
	public void readFields(DataInput in) throws IOException {
	}

	@Override
	public int compareTo(User o) {
		
		return this.id > o.getId() ? -1 : (this.id < o.getId() ? 1 : 0);
	}

}
