package com.xl.hbase.rtc.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.WritableUtils;

public class Select implements Expression {

	//需要查询的列
	private List<String> colNames;
	
	//查询中的计算列在colNames中的下标
	private List<Integer> calPosi;
	
	//列的别名
	private Map<String, String> nameMap ;
	public Select() {
		colNames = new ArrayList<String>();
		calPosi = new ArrayList<Integer>();
		nameMap = new HashMap<String, String>();
	}
	
	public void addColumn(String colName) {
		if (colName.indexOf(Expression.DELIM) != -1) {
			int pos = colName.indexOf(Expression.DELIM);
			String alias=colName.substring(pos+1);
			colName=colName.substring(0, pos);
			nameMap.put(colName, alias);
		}
		colNames.add(colName);
		//this is a calculation column
		if (colName.indexOf(Expression.TOKEN_SP)>0) {
			calPosi.add(colNames.size()-1);
		}
	}
	
	public String getColumn(int i) {
		return colNames.get(i); 
	}
	
	public List<String> getColumns() {
		return colNames;
	}
	
	public String getOutputName(String name) {
		if (nameMap.containsKey(name)) 
			return nameMap.get(name);
		return name;
	}
	
	public List<Integer> getCaculation() {
		return calPosi;
	}
	
	
	@Override 
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		for (int i=0;i<size;i++) {
			colNames.add(WritableUtils.readString(in));
		}
		
		//read the position of caculate columns
		size = in.readInt();
		for (int i=0;i<size;i++) {
			calPosi.add(in.readInt());
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(colNames.size());
		for (int i=0;i<colNames.size();++i) {
			WritableUtils.writeString(out, colNames.get(i));
		}
		
		//write the position of calculate columns
		out.writeInt(calPosi.size());
		for (int i=0;i<calPosi.size();++i) {
			out.writeInt(calPosi.get(i).intValue());
		}
	}
}
