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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.xl.hbase.rtc.expression.OrderBy;

/**
 * @author jiangxl
 *
 */
public class ResultSet implements Writable {

	protected Logger LOG = Logger.getLogger(Column.class);
	
	private List<Row> rows;
	private Map<Integer, Integer> aggvalue;
	
	public ResultSet() {
		rows = new ArrayList<Row>();
		aggvalue = new HashMap<Integer, Integer>();
	}
	
	public  boolean hasAggValue(Integer value) {
		boolean rtn = (aggvalue.size() == 0) ? false : 
				aggvalue.containsKey(value);
		return rtn;
	}
	
	public  Integer getAggValuePosition(Integer value) {
		return aggvalue.get(value);
	}
	
	public  int size() {
		return rows.size();
	}
	
	public  Row get(int idx) {
		return rows.get(idx);
	}
	
	public  void add(int idx, Row row) {
		rows.add(idx, row);
	}
	
	public  void add(Row row) {
		rows.add(row);
	}
	
	public void add(Row[] rs) {
		if (rs!=null)
		for (int i=0;i<rs.length;i++) {
			rows.add(rs[i]);
		}
	}
	
	public void remove(int idx) {
		rows.remove(idx);
	}
	
	public void add(Row row, List<OrderBy> orderbys) {
		add(0, row, orderbys);
	}
	
	private int add(int start, Row row, List<OrderBy> orderbys) {
		int position = size();
		if (orderbys!=null && size() > 0) {//find right position for this row
			int end=size()-1,mid;
			do {
				mid = (start + end) / 2;
				int result = compareByOrder(get(mid), row, orderbys);
				if (result<0) {
					start = mid + 1;
				}
				else if (result > 0) {
					end = mid - 1;
				}
				else { 
					position = mid; break; 
				}
			} while (start <= end);
			if (start > end)
				position = start;
		}
		LOG.debug("add a row at " + position);
		add(position, row);
		return position+1;
	}
	
	private int compareByOrder(Row cRow, Row row, List<OrderBy> orderbys) {
		//handle multiple order by condition one by one\
		for (int j=0; j < orderbys.size();++j) {
			OrderBy cob = orderbys.get(j);
			String colName = cob.getColName();
			
			if (colName == null)
				colName = cob.getAggregate().getFakeName();
			
			int compareReult = cRow.getColumn(colName).compare(row.getColumn(colName), cob);
			if (compareReult > 0) {
				return 1;
			} else if (compareReult<0) return -1;
		}
		return 0;
	}

	public synchronized void merge(ResultSet tempRS, List<OrderBy> orderbys) {
		//merge the result into a big result set
		if (orderbys == null || size() == 0) {
			for (int i=0;i<tempRS.size();i++) {
				add(tempRS.get(i));
			}
		} else {
			if (tempRS.size()==0) return ; //nothing to merge
			int ifinal=0, itemp=0;
			while (true) {
				Row rowInTemp = tempRS.get(itemp);
				ifinal = add(ifinal, rowInTemp, orderbys);
				itemp++;
				if (itemp>=tempRS.size()) break;
				else if (ifinal >= size()) {
					//add all the rest elements in tempRS 
					for (int i=itemp;i<tempRS.size();++i) {
						add(tempRS.get(i));
					}
					break;
				}
			}
		}
	}
	
	public synchronized void accumulate(Row currentRow) {
		
		Column col = currentRow.getColumn(Column.SPLICE_KEY_COL_NAME);
		
		LOG.debug("accumulate the row: "+currentRow.getKeyString());
		
		if (col!=null) {
			int hashKey =  Bytes.hashCode(col.getValue());
			//find the value in the rows;
			LOG.debug("current group by key: " + hashKey);
			if (hasAggValue(hashKey)) {
				Integer pos = getAggValuePosition(hashKey);
				//fetch each aggregate column, do accumulation
				Row saveTo = get(pos);
				List<String> aggColumns = currentRow.getAggColumns();
				for (int i=0;i<aggColumns.size();++i) {
					String name = aggColumns.get(i);
					saveTo.getColumn(name).accumulate(
							currentRow.getColumn(name));
				}
			} else {
				//create revert index and save column
				LOG.debug("add a group by key: " + hashKey);
				aggvalue.put(hashKey, size());
				add(currentRow);
			}
		} else {
			//no group by columns, aggregate all to one row;
			if (rows.size() == 0) {
				add(currentRow);
			} else {
				List<String> aggColumns = currentRow.getAggColumns();
				Row saveTo = rows.get(0);
				//skip the non-aggregate columns
				for (int i=0;i<aggColumns.size();++i) {
					String name = aggColumns.get(i);
					if (saveTo.getColumn(name)!=null)
						saveTo.getColumn(name).accumulate(
							currentRow.getColumn(name));
				}
			}
		}
	}
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		//read row
		int size = in.readInt();
		for (int i=0;i<size;++i) {
			Row row = new Row();
			row.readFields(in);
			rows.add(row);
		}
		//read aggvalue
		size = in.readInt();
		for (int i=0;i<size;++i) {
			int key=in.readInt();
			int pos = in.readInt();
			aggvalue.put(key, new Integer(pos));
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//write row
		out.writeInt(rows.size());
		for (int i=0;i<rows.size();++i) {
			rows.get(i).write(out);
		}
		
		//write aggvalue
		out.writeInt(aggvalue.size());
		Iterator<Integer> ib = aggvalue.keySet().iterator();
		while (ib.hasNext()) {
			Integer key = ib.next();
			out.writeInt(key.intValue());
			out.writeInt(aggvalue.get(key).intValue());
		}
	}

	public ResultSet getTop(int top) {
		if (top<0) return this;
		if (size() < top) top = size();
		ResultSet rtn = new ResultSet();
		for (int i=0;i<top;i++) {
			rtn.add(get(i));
		}
		return rtn;
	}
	
	public void print(PrintStream printer) {
		if (rows.size()==0) printer.print("empty");
		for (int i=0;i<rows.size();i++) {
			Row row = rows.get(i);
			row.print(System.out);
			printer.println();
		}
	}
}
