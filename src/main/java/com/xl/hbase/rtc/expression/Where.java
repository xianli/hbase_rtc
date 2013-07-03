package com.xl.hbase.rtc.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

/**
 * _row_* is keyword in where expression. e.g., _row_0 is the first part of row key.
 * _row_1 is the second part... 
 * @author jiangxl
 *
 */
public class Where implements Expression {
	
	private Where left;
	private Where right;
	private String colName;
	private byte[] expectedValue;
	private int operator;
	
	public Where() {}
	
	public Where(Where left, Where right, int operator) {
		this.left = left;
		this.right = right;
		this.operator = operator;
	}
	
	public Where(String colName, String value, int operator) {
		this.colName = colName;
		this.expectedValue = Bytes.toBytes(value);
		this.operator = operator;
	}
	
	public Where(String colName, int value, int operator) {
		this.colName = colName;
		this.expectedValue = Bytes.toBytes(value);
		this.operator = operator;
	}
	
	public Where(String colName, long value, int operator) {
		this.colName = colName;
		this.expectedValue = Bytes.toBytes(value);
		this.operator = operator;
	}
	
	public Where(String colName, double value, int operator) {
		this.colName = colName;
		this.expectedValue = Bytes.toBytes(value);
		this.operator = operator;
	}
	
	public Where(String colName, byte value, int operator) {
		this.colName = colName;
		this.expectedValue = new byte[1];
		expectedValue[0] = value;
		this.operator = operator;
	}
	
	public String getColName() {
		return colName;
	}

	public byte[] getExpectedValue() {
		return expectedValue;
	}

	public int getOperator() {
		return operator;
	}

	public Where getLeft() {
		return left;
	}

	public Where getRight() {
		return right;
	}
	

	@Override
	public void readFields(DataInput in) throws IOException {
		operator = in.readInt();
		if (operator == LOGIC_AND || operator == LOGIC_OR) {
			Where left = new Where();
			left.readFields(in);
			this.left = left;
			Where right = new Where();
			right.readFields(in);
			this.right = right;
		} else {
			colName = WritableUtils.readString(in);
			int length = in.readInt();
			expectedValue = new byte[length];
			in.readFully(expectedValue);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//write operator first;
		out.writeInt(operator);
		if (operator == LOGIC_AND || operator == LOGIC_OR) {
			left.write(out);
			right.write(out);
		} else {
			WritableUtils.writeString(out, colName);
			int len = expectedValue.length;
			out.writeInt(len);
			out.write(expectedValue);
		}
	}
}
