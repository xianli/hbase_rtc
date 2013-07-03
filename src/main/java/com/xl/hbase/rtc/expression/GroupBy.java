package com.xl.hbase.rtc.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.WritableUtils;

public class GroupBy implements Expression {

	private List<String> colNames;
	
	public GroupBy() {
		colNames = new ArrayList<String>();
	}
	
	public void addColumn(String column) {
		colNames.add(column);
	}

	public List<String> getColumns() {
		return colNames;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		for (int i=0;i<size;i++) {
			colNames.add(WritableUtils.readString(in));
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//first write size
		out.writeInt(colNames.size());
		for (int i=0;i<colNames.size();++i) {
			WritableUtils.writeString(out, colNames.get(i));
		}
	}
}
