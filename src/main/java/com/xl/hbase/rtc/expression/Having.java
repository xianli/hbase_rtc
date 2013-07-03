package com.xl.hbase.rtc.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;


public class Having implements Expression {
	
	private Having left;
	private Having right;
	private Aggregate aggCol;
	private byte[] expectedValue;
	private int operator;
	
	public Having() {}
	
	public Having(Having left, Having right, int operator) {
		this.left = left;
		this.right = right;
		this.operator = operator;
	}
	
	public Having(Aggregate colName, String value, int operator) {
		this.aggCol = colName;
		this.expectedValue = Bytes.toBytes(value);
		this.operator = operator;
	}
	
	public Having(Aggregate colName, int value, int operator) {
		this.aggCol = colName;
		this.expectedValue = Bytes.toBytes(value);
		this.operator = operator;
	}
	
	public Having(Aggregate colName, long value, int operator) {
		this.aggCol = colName;
		this.expectedValue = Bytes.toBytes(value);
		this.operator = operator;
	}
	
	public Having(Aggregate colName, double value, int operator) {
		this.aggCol = colName;
		this.expectedValue = Bytes.toBytes(value);
		this.operator = operator;
	}
	
	public Having(Aggregate colName, byte value, int operator) {
		this.aggCol = colName;
		this.expectedValue = new byte[1];
		expectedValue[0] = value;
		this.operator = operator;
	}
	
	public Aggregate getAggregate() {
		return aggCol;
	}

	public byte[] getExpectedValue() {
		return expectedValue;
	}

	public int getOperator() {
		return operator;
	}

	public Having getLeft() {
		return left;
	}

	public Having getRight() {
		return right;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		operator = in.readInt();
		if (operator == LOGIC_AND || operator == LOGIC_OR) {
			Having left = new Having();
			left.readFields(in);
			this.left = left;
			Having right = new Having();
			right.readFields(in);
			this.right = right;
		} else {
			Aggregate agg = new Aggregate();
			agg.readFields(in);
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
			aggCol.write(out);
			out.writeInt(expectedValue.length);
			out.write(expectedValue);
		}
	}
}
