package com.xl.hbase.rtc.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;
/**
 * @author jiangxl
 */
public class Aggregate implements Expression {

	private int type;
	private String colName;
	private String fakeName;
	
	private String alias;
	
	public String getColName() {
		return colName;
	}
	public void setColName(String colName) {
		this.colName = colName;
	}
	public int getType() {
		return type;
	}
	public void setType(int type) {
		this.type = type;
	}

	public static int getTypeByName(String name) {
		if ("sum".equalsIgnoreCase(name)) {
			return Expression.AGG_SUM;
		} else if ("avg".equalsIgnoreCase(name)) {
			return Expression.AGG_AVG;
		} else if ("count".equalsIgnoreCase(name)) {
			return Expression.AGG_COUNT;
		} else if ("max".equalsIgnoreCase(name)) {
			return Expression.AGG_MAX;
		} else if ("min".equalsIgnoreCase(name)) {
			return Expression.AGG_MIN;
		} else if ("div".equalsIgnoreCase(name)) {
			return Expression.AGG_DIV;
		} else if ("out".equalsIgnoreCase(name)) {
			return Expression.AGG_OUT;
		} else if ("cal".equalsIgnoreCase(name)) {
			return Expression.AGG_CAL;
		}  else return -1;
	}
	
	public String getFakeName() {
		return fakeName;
	}
	
	public String getOutputName() {
		return (alias!=null)?alias:fakeName;
	}
	
	/**
	 * generate a fake name for aggregation column so that
	 * this column can be placed in result set
	 * @return
	 */
	private String getInternalFakeName() {
		String name=null;
		switch (type) {
			case Expression.AGG_AVG:
				name = PREFIX_AVG+colName;
				break;
			case Expression.AGG_COUNT:
				name = PREFIX_COUNT+colName;
				break;
			case Expression.AGG_MAX:
				name = PREFIX_MAX+colName;
				break;
			case Expression.AGG_MIN:
				name = PREFIX_MIN+colName;
				break;
			case Expression.AGG_SUM:
				name = PREFIX_SUM+colName;
				break;
			case Expression.AGG_DIV:
				name = PREFIX_DIV+colName;
				break;
			case Expression.AGG_OUT:
				name = PREFIX_OUT+colName;
				break;
			case Expression.AGG_CAL:
				name = colName;
				break;
		}
		return name;
	}
	
	public static final String PREFIX_AVG="_#avg#";
	public static final String PREFIX_SUM="_#sum#";
	public static final String PREFIX_COUNT="_#cnt#";
	public static final String PREFIX_MAX="_#max#";
	public static final String PREFIX_MIN="_#min#";
	public static final String PREFIX_DIV="_#div#";
	public static final String PREFIX_OUT="_#out#";
	public static final String PREFIX_CAL="_#cal#";
	public static final int LEN_PREFIX=6;
	
	public static boolean isAggregateCol(String colName) {
		return colName.startsWith(PREFIX_AVG) ||
		colName.startsWith(PREFIX_COUNT) || 
		colName.startsWith(PREFIX_MAX) || 
		colName.startsWith(PREFIX_MIN) || 
		colName.startsWith(PREFIX_SUM) ; 
	}
	
	public Aggregate() {}
	public Aggregate(String colName, int type) {
		this.colName = colName;
		this.type = type;
		this.fakeName = getInternalFakeName();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		type = in.readInt();
		colName = WritableUtils.readString(in);
		fakeName = WritableUtils.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(type);
		WritableUtils.writeString(out, colName);
		WritableUtils.writeString(out, fakeName);
	}
	public void setAlias(String alias) {
		this.alias = alias;
	}
	public String getAlias() {
		return alias;
	}
}
