package com.xl.hbase.rtc.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

import com.xl.hbase.rtc.expression.Aggregate;
import com.xl.hbase.rtc.expression.Expression;
import com.xl.hbase.rtc.expression.Select;

/**
 * @author jiangxl
 */
public class Row implements Writable {
	
	protected Logger LOG = Logger.getLogger(Row.class);
	
	private byte[] key;
	private Map<String, Column> store;
	public List<String> aggColumns;
	private String keyString;
	public Row() {
		store = new HashMap<String, Column>();
		aggColumns = new ArrayList<String>();
	}
	
	public Set<String> getColumnNames() {
		return store.keySet();
	}
	
	public void addColumn(Column col) {
		store.put(col.getName(), col);
		
		if (col.getAggType() != -1) {
			int pos=aggColumns.size();
			aggColumns.add(pos, col.getName());
		}
	}
	
	public void setKey(byte[] pk) {
		key = pk;
	}
	
	public List<String> getAggColumns() {
		return aggColumns;
	}
	
	public Column getColumn(String colName) {
		return store.get(colName);
	}
	
	public byte[] getKey() {
		return key;
	}

	public Row get(Select sel) {
		Row rtn = new Row();
		rtn.setKey(key);
		
		List<String> colNames = sel.getColumns();
		for (int i=0;i<colNames.size();++i) {
			rtn.addColumn(store.get(colNames.get(i)));
		}
		return rtn;
	}
	
	public Row get(Select sel, List<Aggregate> aggs) {
		return null;
	}

	public boolean hasColumn(String colName) {
		return (store.containsKey(colName));
	}
	
	public String getKeyString() {
		return keyString;
	}
	
	public void setkeyString(String kt) {
		keyString = kt;
	}
	
	public void doCaculation(String suffixExp) {
		String[] tokens = suffixExp.split("\\"+Expression.TOKEN_SP);
		Stack<Column> operand = new Stack<Column>();
		byte[] value=null;
		for (int i=0;i<tokens.length;++i) {
			if (tokens[i].equals("+")) {
				Column rightCol = operand.pop();
				Column leftCol = operand.pop();
				value=leftCol.plus(rightCol);
				operand.push(new Column(
							suffixExp, value, Column.DataType.DOUBLE));
			} else if (tokens[i].equals("-")) {
				Column rightCol = operand.pop();
				Column leftCol = operand.pop();
				value=leftCol.minus(rightCol);
				operand.push(new Column(
						suffixExp, value, Column.DataType.DOUBLE));
			} else if (tokens[i].equals("*")) {
				Column rightCol = operand.pop();
				Column leftCol = operand.pop();
				value=leftCol.multiply(rightCol);
				operand.push(new Column(
						suffixExp, value, Column.DataType.DOUBLE));
			} else if (tokens[i].equals("/")) {
				Column rightCol = operand.pop();
				Column leftCol = operand.pop();
				value=leftCol.divide(rightCol);
				operand.push(new Column(
						suffixExp, value, Column.DataType.DOUBLE));
			} else if (tokens[i].matches(Expression.DIGIT)) {
				double v = Double.parseDouble(tokens[i]);
				operand.push(new Column(
						suffixExp, Bytes.toBytes(v), Column.DataType.DOUBLE));
			} else {
				operand.push(getColumn(tokens[i]));
			}
		}
		
		if (operand.size()==1) {
			Column last=operand.pop();
			LOG.debug("calculate " + suffixExp +" = " + Bytes.toDouble(last.getValue()));
			addColumn(last);
		} else {
			throw new RuntimeException("calulate error: " + suffixExp );
		}
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int keyLen = in.readInt();
		if (keyLen>-1) {
			key = new byte[keyLen];
			in.readFully(key);
		}
		
		int size = in.readInt();
		//read store
		for (int i=0;i<size;++i) {
			String colName = WritableUtils.readString(in);
			Column col = new Column();
			col.readFields(in);
			store.put(colName, col);
		}
		
		//read aggColumns
		size = in.readInt();
		for (int i=0;i<size;i++) {
			aggColumns.add(WritableUtils.readString(in));
		}
		keyString = WritableUtils.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		if (key==null)
			out.writeInt(-1);
		else { 
			int keyLen = key.length;
			out.writeInt(keyLen);
			out.write(key);
		}
		
		//write store
		out.writeInt(store.size()); //how many columns in the row
		Iterator<String> it = store.keySet().iterator();
		while (it.hasNext()) {
			String colName = it.next();
			Column col = store.get(colName);
			WritableUtils.writeString(out, colName);
			col.write(out);
		}
		//write aggColumns
		out.writeInt(aggColumns.size());
		for (int i=0;i<aggColumns.size();++i) {
			WritableUtils.writeString(out, aggColumns.get(i));
		}
		WritableUtils.writeString(out, keyString);
	}
	
	
	public void print(PrintStream printer) {
		Set<String> names = getColumnNames();
		Iterator<String> in = names.iterator();
		printer.print(getKeyString() + " | ");
		while (in.hasNext()) {
			Column col = getColumn(in.next());
			if (col.getType().equals(Column.DataType.INT)) {
				printer.print(col.getName()+":"+Bytes.toInt(col.getValue()) + " | ");
			} else if (col.getType().equals(Column.DataType.LONG)) {
				printer.print(col.getName());
				printer.print(":"+Bytes.toLong(col.getValue()) + " | ");
			} else if (col.getType().equals(Column.DataType.DOUBLE)) {
				printer.print(col.getName()+":"+Bytes.toDouble(col.getValue()) + " | ");
			} else if (col.getType().equals(Column.DataType.STRING)) {
				printer.print(col.getName()+":"+Bytes.toString(col.getValue()) + " | ");
			} else if (col.getType().equals(Column.DataType.BYTE)) {
				printer.print(col.getName()+":"+col.getValue()[0] + " | ");
			}
		}
	}
	
}
