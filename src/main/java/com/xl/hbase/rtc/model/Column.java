package com.xl.hbase.rtc.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

import com.xl.hbase.rtc.expression.Expression;
import com.xl.hbase.rtc.expression.OrderBy;

public class Column implements Writable {
	
	protected Logger LOG = Logger.getLogger(Column.class);
	
	private String name;
	private byte[] value;
	private String type;
	private int aggType;
	private long timestamp;
	
	public Column() {}
	
	public Column(String name, byte[] value, String type) {
		this.name = name;
		this.value = value;
		this.type = type;
		this.aggType = -1;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	public byte[] getValue() {
		return value;
	}
	public void setValue(byte[] value) {
		this.value = value;
	}
	
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	
	public static final String SPLICE_KEY_COL_NAME="##aggkey";
	
	public interface DataType {
		
		public String BYTE="byte";
		public String BYTES="bytes";
		public String INT="int";
		public String LONG= "long";
		public String DOUBLE= "double";
		public String STRING="string"; 
		
		/** a special key used in aggregate computation*/
		public String AGGKEY="aggkey";
	}

	
	public static int getFixLength(String datatype) {
		if (DataType.INT.equals(datatype)) {
			return 4;
		} else if (DataType.LONG.equals(datatype)) {
			return 8;
		} else if (DataType.DOUBLE.equals(datatype)) {
			return 8;
		} else if (DataType.BYTE.equals(datatype)) {
			return 1;
		} else if (DataType.STRING.equals(datatype)) {
			return -1;
		}  else if (DataType.BYTES.equals(datatype)) {
			return -1;
		}  return 0;
	}
	
	public void setAggType(int aggType) {
		this.aggType = aggType;
		switch (aggType) {
			case Expression.AGG_AVG: 
				type = DataType.DOUBLE; 
				break;
			case Expression.AGG_COUNT: 
				type = DataType.LONG;	
				value = Bytes.toBytes(1l);
				break;
			case Expression.AGG_SUM: 	
				if (type.equals(DataType.INT)) {
					int v = Bytes.toInt(value);
					value = Bytes.toBytes((long)v);
					type = DataType.LONG;
				}
				break;
		//no data type change is needed for max and min
		}
	}
	
	public void setAggType(String aggType) {
		if ("sum".equalsIgnoreCase(aggType)) {
			setAggType(Expression.AGG_SUM);
		} else if ("avg".equalsIgnoreCase(aggType)) {
			setAggType(Expression.AGG_AVG);
		} else if ("count".equalsIgnoreCase(aggType)) {
			setAggType(Expression.AGG_COUNT);
		} else if ("max".equalsIgnoreCase(aggType)) {
			setAggType(Expression.AGG_MAX);
		} else if ("min".equalsIgnoreCase(aggType)) {
			setAggType(Expression.AGG_MIN);
		} 
	}
	/**
	 * 
	 * @return -1: not aggregate column, >0: aggregate columns
	 */
	public int getAggType() {
		return aggType;
	}

	public void accumulate(Column col) {
		if (col == null) return ;
		switch (aggType) {
//			case Expression.AGG_AVG:
//				//do nothing
//				break;
			case Expression.AGG_COUNT:
					value = internalAdd(value, col.getValue(), type);
				break;
			case Expression.AGG_MAX:
				if (Bytes.compareTo(value, col.getValue()) < 0)
					value = col.getValue();
				break;
			case Expression.AGG_MIN:
				if ( Bytes.compareTo(value, col.getValue()) > 0)
					value = col.getValue();
				break;
			case Expression.AGG_SUM:
					value = internalAdd(value, col.getValue(), type);
				break;
		}
	}
	
	private byte[] internalAdd(byte[] par, byte[] chi, String dataType) {
		if  (DataType.INT.equals(dataType)) {
			int parent=Bytes.toInt(par);
			int child=Bytes.toInt(chi);
			long result = (long)parent + (long)child;
			return Bytes.toBytes(result);
		} else if(DataType.DOUBLE.equals(dataType)) {
			double parent=Bytes.toDouble(par);
			double child=Bytes.toDouble(chi);
			double result = parent + child;
			return Bytes.toBytes(result);
		} else if (DataType.LONG.equals(dataType)) {
			long parent=Bytes.toLong(par);
			long child=Bytes.toLong(chi);
			long result = parent + child;
			return Bytes.toBytes(result);
		} else return null;
	}
	
	public byte[] plus(Column anotherCol) {
		byte[] numerator = value;
		byte[] denominator=anotherCol.getValue();
		double parent = getParentValue(numerator, type);
		
		if(DataType.DOUBLE.equals(anotherCol.getType())) {
			double child=Bytes.toDouble(denominator);
			double result = parent + child;
			LOG.debug(parent + " + " + child + " = " + result);
			return Bytes.toBytes(result);
		} else if (DataType.LONG.equals(anotherCol.getType())) {
			long child=Bytes.toLong(denominator);
			double result = parent + child;
			LOG.debug(parent + " + " + child + " = " + result);
			return Bytes.toBytes(result);
		} else if (DataType.INT.equals(anotherCol.getType())) {
			int child=Bytes.toInt(denominator);
			double result = parent + child;
			LOG.debug(parent + " + " + child + " = " + result);
			return Bytes.toBytes(result);
		} else return null;
	}
	
	public byte[] minus(Column anotherCol) {
		byte[] numerator = value;
		byte[] denominator=anotherCol.getValue();
		double parent = getParentValue(numerator, type);
		
		if(DataType.DOUBLE.equals(anotherCol.getType())) {
			double child=Bytes.toDouble(denominator);
			double result = parent - child;
			LOG.debug(parent + " - " + child + " = " + result);
			return Bytes.toBytes(result);
		} else if (DataType.LONG.equals(anotherCol.getType())) {
			long child=Bytes.toLong(denominator);
			double result = parent - child;
			LOG.debug(parent + " - " + child + " = " + result);
			return Bytes.toBytes(result);
		} else if (DataType.INT.equals(anotherCol.getType())) {
			int child=Bytes.toInt(denominator);
			double result = parent - child;
			LOG.debug(parent + " - " + child + " = " + result);
			return Bytes.toBytes(result);
		} else return null;
	}
	
	public byte[] multiply(Column anotherCol) {
		byte[] numerator = value;
		byte[] denominator=anotherCol.getValue();
		double parent = getParentValue(numerator, type);
		
		if(DataType.DOUBLE.equals(anotherCol.getType())) {
			double child=Bytes.toDouble(denominator);
			double result = parent * child;
			LOG.debug(parent + " * " + child + " = " + result);
			return Bytes.toBytes(result);
		} else if (DataType.LONG.equals(anotherCol.getType())) {
			long child=Bytes.toLong(denominator);
			double result = parent * child;
			LOG.debug(parent + " * " + child + " = " + result);
			return Bytes.toBytes(result);
		} else if (DataType.INT.equals(anotherCol.getType())) {
			int child=Bytes.toInt(denominator);
			double result = parent * child;
			LOG.debug(parent + " * " + child + " = " + result);
			return Bytes.toBytes(result);
		} else return null;
		
	}
	
	private double getParentValue(byte[] numerator, String dt) {
		double parent=0.0;
		if(DataType.DOUBLE.equals(dt)) {
			parent=Bytes.toDouble(numerator);
		} else if (DataType.LONG.equals(dt)) {
			parent= (double)Bytes.toLong(numerator);
		} else if (DataType.INT.equals(dt)) {
			parent = (double) Bytes.toInt(numerator);
		}
		return parent;
	}
	
	public byte[] divide(Column anotherCol) {
		byte[] numerator = value;
		byte[] denominator=anotherCol.getValue();
		double parent = getParentValue(numerator, type);
		
		if(DataType.DOUBLE.equals(anotherCol.getType())) {
			double child=Bytes.toDouble(denominator);
			double result;
			if (child==0d) {
				result = 0;
				LOG.warn("divide 0 with "+parent);
			}
			else result = parent / child;
			LOG.debug(parent + " / " + child + " = " + result);
			return Bytes.toBytes(result);
		} else if (DataType.LONG.equals(anotherCol.getType())) {
			long child=Bytes.toLong(denominator);
			double result;
			if (child==0l) {
				result = 0;
				LOG.warn("divide 0 with "+parent);
			}
			else result = parent / child;
			LOG.debug(parent + " / " + child + " = " + result);
			return Bytes.toBytes(result);
		} else if (DataType.INT.equals(anotherCol.getType())) {
			int child=Bytes.toInt(denominator);
			double result;
			if (child==0) {
				result = 0;
				LOG.warn("divide 0 with "+parent);
			}
			else result = parent / child;
			LOG.debug(parent + " / " + child + " = " + result);
			return Bytes.toBytes(result);
		} else return null;
	}
	
	public int compare(Column another, OrderBy ob) {
		byte[] in = getValue();
		byte[] out = another.getValue();
		int orderType = ob.getType();
		int compareResult = Bytes.compareTo(in, out);
		
		int ret = -2; 
		if (orderType == Expression.ORD_ASC) {
			if (compareResult > 0)  ret = 1;	//can stop compare now
			else if (compareResult < 0) ret = -1;
			else ret = 0;
			//else compareResult == 0; nothing to do;
		} else if (orderType == Expression.ORD_DESC) {
			if (compareResult < 0) 	return ret = 1;
			else if (compareResult > 0) return ret = -1;
			else return ret = 0;
		} 
		
		LOG.debug("compare " + name + ":"+  getReadableValue() + " with " + 
				another.getReadableValue() + " result: " + ret);
		return ret;
	}
	
	
	public Object getReadableValue() {
		if (type.equals("int")) {
			return Bytes.toInt(value);
		} else if (type.equals("long")) {
			return Bytes.toLong(value);
		} else if (type.equals("double")) {
			return Bytes.toDouble(value);
		}  else if (type.equals("string")) {
			return Bytes.toString(value);
		}  else if (type.equals("byte")) {
			return value[0];
		} else if (type.equals("bytes")) {
			return Bytes.toStringBinary(value, 0, value.length);
		} 
		return null;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		name = WritableUtils.readString(in);
		type = WritableUtils.readString(in);
		aggType = in.readInt();
		//write a length first
		int valueLen = in.readInt();
		if (valueLen>-1) {
			value = new byte[valueLen];
			in.readFully(value);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, name);
		WritableUtils.writeString(out, type);
		out.writeInt(aggType);
		//write a length first
		if (value==null) out.writeInt(-1);
		else {
			out.writeInt(value.length);
			out.write(value);
		}
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public long getTimestamp() {
		return timestamp;
	}
}
