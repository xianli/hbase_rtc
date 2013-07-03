package com.xl.hbase.rtc.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;

/**
 * @author jiangxl
 */
public class OrderBy implements Expression {

	private String colName;
	private Aggregate aggCol;
	private int type;
	
	public OrderBy() {}
	
	public OrderBy(String col, int orderType) {
		this.colName = col;
		this.type = orderType;
	}
	
	public OrderBy(Aggregate col, int orderType) {
		this.aggCol = col;
		this.type = orderType;
	}
	
	public Aggregate getAggregate() {
		return aggCol;
	}

	public void setAggregate(Aggregate aggCol) {
		this.aggCol = aggCol;
	}

	public String getColName() {
		return colName;
	}

	public int getType() {
		return type;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		type = in.readInt();
		//read a flag first
		int flag = in.readInt();
		if (flag == 1) {   
			colName = WritableUtils.readString(in);
		} else if (flag == 2) {
			aggCol  = new Aggregate();
			aggCol.readFields(in);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(type);
		if (colName != null) {
			out.writeInt(1);
			WritableUtils.writeString(out, colName);
		} else if (aggCol != null) {
			out.writeInt(2);
			aggCol.write(out);
		}
		
	}
}
