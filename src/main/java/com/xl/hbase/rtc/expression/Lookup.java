package com.xl.hbase.rtc.expression;


import com.jd.bi.odp.data.hbase.inf.SortedKVMap;

public class Lookup {
	private String colName;
	private SortedKVMap dim;
	private String alias;
	
	public String getAlias() {
		return alias;
	}
	public void setAlias(String alias) {
		this.alias = alias;
	}
	public String getColName() {
		return colName;
	}
	public void setColName(String colName) {
		this.colName = colName;
	}
	public void setDim(SortedKVMap dim) {
		this.dim = dim;
	}
	public SortedKVMap getDim() {
		return dim;
	}

}
