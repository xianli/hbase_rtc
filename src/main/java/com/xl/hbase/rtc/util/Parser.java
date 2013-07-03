package com.xl.hbase.rtc.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.xl.hbase.rtc.expression.AggSelect;
import com.xl.hbase.rtc.expression.Aggregate;
import com.xl.hbase.rtc.expression.Expression;
import com.xl.hbase.rtc.expression.GroupBy;
import com.xl.hbase.rtc.expression.Having;
import com.xl.hbase.rtc.expression.OrderBy;
import com.xl.hbase.rtc.expression.Select;
import com.xl.hbase.rtc.expression.Where;
import com.xl.hbase.rtc.model.Column;
import com.xl.hbase.rtc.model.MetaData;
import com.xl.hbase.rtc.model.Row;

public class Parser {
	
	protected static Logger LOG = Logger.getLogger(Parser.class);
	
	public static List<String> extractColumnNames( Select sel, AggSelect aggSel,
			Where whereCondition, GroupBy groupbys, Having havings, List<OrderBy> orderbys) {
		
		List<String> saveMap = new ArrayList<String>();
		
		//handle select columns
		if (sel!=null)
			extractColName(sel.getColumns(), saveMap);
		
		//handle aggregate columns
		if (aggSel!=null)
			extractColName(aggSel, saveMap);
		
		//handle filter
		extractColName(whereCondition, saveMap);
		//handle groupbys
		if (groupbys != null)
			extractColName(groupbys.getColumns(), saveMap);
		//handle havings
		extractColName(havings, saveMap, aggSel);
		//handle orderbys
		for (int i=0;orderbys != null && i<orderbys.size();++i) {
			OrderBy tmp = orderbys.get(i);
			if (tmp.getColName() != null)
				addIfNotExist(tmp.getColName(), saveMap);
			else if (tmp.getAggregate() != null)
				addIfNotExist(tmp.getAggregate(),  saveMap, aggSel);
		}
		return saveMap;
	}

	private static void extractColName(AggSelect aggsel,
			List<String> saveMap) {
		List<Aggregate> aggCols = aggsel.getColumns();
		for (int i=0;aggCols != null && i<aggCols.size();++i) {
			addIfNotExist(aggCols.get(i), saveMap, aggsel);
		}
	}
	
	private static  void extractColName(List<String> columns, List<String> saveMap) {
		if (columns == null) return;
		
		for (int i=0;i<columns.size();++i) {
			addIfNotExist(columns.get(i), saveMap);
		}
	}
	
	private static void extractColName(Having havings, List<String> saveMap, AggSelect aggsel) {
		if (havings==null) return;
		
		Aggregate cn = havings.getAggregate();
		if (cn!=null ) {
			addIfNotExist(cn, saveMap, aggsel);
		} else  {
			if (havings.getLeft()!=null)
				extractColName(havings.getLeft(), saveMap, aggsel);
			if (havings.getRight() != null)
				extractColName(havings.getRight(), saveMap, aggsel);
		}
	} 
	
	private static void extractColName(Where condition, List<String> saveMap) {
		if (condition == null) return;
		String cn = condition.getColName();
		if (cn!=null) 
			addIfNotExist(cn, saveMap);
		else {
			if (condition.getLeft() != null)
				extractColName(condition.getLeft(), saveMap);
			if (condition.getRight() != null)
				extractColName(condition.getRight(), saveMap);
		}
	}
	
	private static void addIfNotExist(String name, List<String> saveMap) {
		// get name;
		if (isCalculation(name)) {
			String[] names = name.split("\\"+Expression.TOKEN_SP);
			for (int j=0;		j<names.length && 
								!isOperator(names[j]) && 
								!names[j].matches(Expression.DIGIT);
				++j) {
				if (!saveMap.contains(names[j])) {
					saveMap.add(names[j]);
				}
			}
			
		} else {
			if (!saveMap.contains(name) && 
					!name.startsWith(MetaData.PREF_ROW)) { //no need to add row key columns
				saveMap.add(name);
			}
		}
	}
	
	private static void addIfNotExist(Aggregate agg, List<String> saveMap, AggSelect select) {
		if (isCalculation(agg.getColName())) {
			String[] names = agg.getColName().split("\\"+Expression.TOKEN_SP);
			for (int j=0;j<names.length && 
						!isOperator(names[j])&& 
						!names[j].matches(Expression.DIGIT);
			++j) {
				String colName = names[j].substring(Aggregate.LEN_PREFIX);
				String aggName = names[j].substring(2,5);
				if (!saveMap.contains(names[j])) {
					saveMap.add(colName);
				}
				if (select!=null && !select.contains(names[j])) {
					//add it to the select object
					select.addColumn(new Aggregate(colName, 
							Aggregate.getTypeByName(aggName)));
				}
			}
		} else {
			String name = agg.getColName();
			if (!saveMap.contains(name) && 
					!name.startsWith(MetaData.PREF_ROW)) { //no need to add row key columns
				saveMap.add(name);
			}
		}
	}
	
	
	/**
	 * every element in the KeyValue array 'rawResult' should have same length. e.g., 
	 * if the first KeyValue has four int, then the second must have four long/int/double/etc...
	 * @param rawResult 
	 * @param meta
	 * @param selected 
	 * @return
	 */
	public static Row[] extractRow(List<KeyValue> rawResult, MetaData meta, List<Integer> selected, List<String> columns) {
		Row[] rows = null;
		
		//iterator over all columns
		for (int i=0;i<rawResult.size();i++) {
			KeyValue kv = rawResult.get(i);
			String colName = Bytes.toString(kv.getFamily())+":"+
						Bytes.toString(kv.getQualifier());
			
			if (columns!=null && !columns.contains(colName)) 
				continue;

			byte[] whole = kv.getValue();
			//the first 4 bytes is count;
			int count = Bytes.toInt(Arrays.copyOfRange(whole, 0, 4));
			if (rows==null) {
				rows = (selected==null) ? new Row[count]: 
					new Row[selected.size()];
			}
			
			String type = meta.getColType(colName);
			int length = Column.getFixLength(type);
			int offset=4;
			int point=0;
			//add columns
			for (int j=0;j<count;j++) {
				if (Column.DataType.STRING.equals(type) || Column.DataType.BYTES.equals(type)) {
					//this is a string
					length = Bytes.toInt(Arrays.copyOfRange(whole, offset, offset+4));
					offset += 4;
				}
				if (selected != null && !selected.contains(j+1)) {
					offset+=length;
					continue;
				}
				
				//initialize current row
				if (rows[point]==null) 
					rows[point] = new Row();
				//generate this column
				byte[] value = Arrays.copyOfRange(whole, offset, offset+length);
				Column col = new Column(colName, value, type);
				
				//add it into current row
				rows[point].addColumn(col);
				
				if (rows[point].getKey() == null) {
					rows[point].setKey(kv.getRow());
					String rk = Util.bytes2String(kv.getRow(), meta.getRowkeyType());
					rows[point].setkeyString(rk);
					addRowkeyColumn(rows[point], kv.getRow(), meta.getRowkeyType());
				}
				point++;
				offset+=length;
			}
		}
		return rows;
	}
	
	public static void addRowkeyColumn(Row row, byte[] v, String type) {
		String[] types = type.split("\\,");
		for (int j=0, start=0;j<types.length;j++) {
			String name = MetaData.PREF_ROW+j;
			if (types[j].equals(Column.DataType.INT)) {
				row.addColumn(new Column(name, 
				Arrays.copyOfRange(v, start, start+4),Column.DataType.INT));
				start+=4;
			} else if (types[j].equals(Column.DataType.LONG)) {
				row.addColumn(new Column(name, 
				Arrays.copyOfRange(v, start, start+8),Column.DataType.LONG));
				start+=8;
			} else if (types[j].equals(Column.DataType.BYTE)) {
				row.addColumn(new Column(name, 
				Arrays.copyOfRange(v, start, start+1),Column.DataType.BYTE));
				start+=1;
			} else if (types[j].equals(Column.DataType.STRING)) {
				int len  = Bytes.toInt(Arrays.copyOfRange(v, start, start+4));
				start += 4;
				row.addColumn(new Column(name, 
						Arrays.copyOfRange(v, start, start+len),Column.DataType.STRING));
				start+=len;
			} else if (types[j].equals(Column.DataType.BYTES)) {
				int len  = Bytes.toInt(Arrays.copyOfRange(v, start, start+4));
				start += 4;
				row.addColumn(new Column(name, 
						Arrays.copyOfRange(v, start, start+len),Column.DataType.BYTES));
				start+=len;
			}
		}
	}
	
	public static boolean isCalculation(String colName) {
		return colName.indexOf(Expression.TOKEN_SP) <0 ? false : true;
	}
	
	public static boolean isOperator(String token) {
		return token.equals("+") || token.equals("-") 
		|| token.equals("*")||token.equals("/");
	}
}
