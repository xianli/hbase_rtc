package com.xl.hbase.rtc.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.jd.bi.odp.data.hbase.reader.ColumnMeta;


/**
 * @author jiangxl
 */
public class MetaData implements Writable {

	public static final String TBL_META="_META_INFO";
	public static final String RKT_NAME="_RowKeyType";
	public static final String PREF_ROW="_row_";
	
	private String rowkeyType;
	private String tableName;
	
	/**data type of each columns*/
	private Map<String, String> colTypes;
	/**if a column has index*/
	private Map<String, Boolean> isIndexed;
	
	private List<String> colNames;
	
	public interface Type {
		long COL_TYPE=10l;
		long HAS_IDX=9l;
	}
	
	public MetaData() {
		colTypes = new HashMap<String, String>();
		isIndexed = new HashMap<String, Boolean>();
		colNames = new ArrayList<String>();
	}
	
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void setRowkeyType(String rowkeyType) {
		this.rowkeyType = rowkeyType;
	}
	
	public String getRowkeyType() {
		return rowkeyType;
	}
	
	public Map<String, String> getColTypes() {
		return colTypes;
	}

	public void addColType(String colName, String colType) {
		colTypes.put(colName, colType);
		colNames.add(colName);
	}
	
	public String getColType(String colName) {
		return colTypes.get(colName);
	}
	
	public void addIsIndexed(String colName, boolean isIdx) {
		isIndexed.put(colName, isIdx);
	}
	
	public Boolean isIndexed(String colName) {
		return isIndexed.get(colName);
	}
	
	public List<String> getColumns() {
		return colNames;
	}
	
	public static MetaData decorate(final String tableName, 
									final List<ColumnMeta> metas) {
		MetaData md = new MetaData();
		md.setTableName(tableName);
		for (int i=0;i<metas.size();i++) {
			ColumnMeta ameta = metas.get(i);
			md.setRowkeyType(ameta.getRowKeyType());
			md.addColType(ameta.getFullColName(), ameta.getType());
		}
		return md;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, tableName );
		WritableUtils.writeString(out, rowkeyType);
		//write colTypes map
		out.writeInt(colTypes.keySet().size());
		Iterator<String> keys = colTypes.keySet().iterator();
		while (keys.hasNext()) {
			String key = keys.next();
			String type = colTypes.get(key);
			WritableUtils.writeString(out, key);
			WritableUtils.writeString(out, type);
		}
		//write  isIndexed map
		out.writeInt(isIndexed.keySet().size());
		keys = isIndexed.keySet().iterator();
		while (keys.hasNext()) {
			String key = keys.next();
			Boolean index = isIndexed.get(key);
			WritableUtils.writeString(out, key);
			out.write(Bytes.toBytes(index.booleanValue()));
		}
		
		//write colNames
		out.writeInt(colNames.size());
		for(int i=0;i<colNames.size();++i) {
			WritableUtils.writeString(out, 
					colNames.get(i));
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		tableName = WritableUtils.readString(in);
		rowkeyType = WritableUtils.readString(in);
		//read coltypes map
		int len = in.readInt();
		for (int i=0;i<len;i++) {
			String key = WritableUtils.readString(in);
			String type = WritableUtils.readString(in);
			colTypes.put(key, type);
		}
		//read isindexed map
		len = in.readInt();
		for (int i=0;i<len;i++) {
			String key = WritableUtils.readString(in);
			Boolean index = in.readBoolean();
			isIndexed.put(key, index);
		}
		//readColNames
		len=in.readInt();
		for (int i=0;i<len;i++) {
			colNames.add(WritableUtils.readString(in));
		}
	}
}
