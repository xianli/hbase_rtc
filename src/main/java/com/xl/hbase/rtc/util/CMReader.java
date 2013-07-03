package com.xl.hbase.rtc.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.jd.bi.odp.data.hbase.inf.CommonUtil;
import com.jd.bi.odp.data.hbase.reader.ColumnMeta;
import com.jd.bi.odp.data.hbase.reader.HBaseRow;
import com.xl.hbase.rtc.model.Column;
import com.xl.hbase.rtc.model.Row;

/**
 * 
 * @author jiangxl
 */
public class CMReader {
	
	
	private LinkedList<Row> queue;
	private int bufferSize = 20;
	public CMReader() {
		queue = new LinkedList <Row>();
	}
	
	public Row next(RegionScanner scanner, 
			List<ColumnMeta> columns, 
			String rowkeyType) throws IOException {
		if (queue.isEmpty()) {
			while (true) { 
				if (queue.size() > bufferSize) break;
				
				List<KeyValue> result = new ArrayList<KeyValue>();
				Row [] rows = null;
				boolean hasMore = scanner.next(result);
				
				Map<String, KeyValue> maps = createMap(result);
				
				for (int i=0;i<columns.size() && result.size()>0;i++) {
					ColumnMeta ameta = columns.get(i);
					KeyValue kv = maps.get(ameta.getFullColName());
					
					if (kv==null) continue;
					String name = ameta.getFullColName();
					byte[] whole = kv.getValue();
					String type = ameta.getType();
					//the first 4 bytes is length;
					int count = Bytes.toInt(Arrays.copyOfRange(whole, 0, 4));
					int length = Column.getFixLength(type);
					List<Integer> selected = HBaseRow.patternAnalysis(ameta.getChoose(), count);
					if (rows == null) {
						rows = new Row[selected.size()];
						for (int k=0;k<rows.length;k++) {
							rows[k]=new Row();
						}
					}
					
					int offset=4;
					int point=0;
					for (int j=0;j<count;j++) {
						if (length == -1) {
							//this is a string
							length = Bytes.toInt(Arrays.copyOfRange(whole, offset, offset+4));
							offset += 4;
						} 
						if (selected.contains(j+1)) {
							byte[] value = Arrays.copyOfRange(whole, offset, offset+length);
							Column col = new Column(name, value, type);
							rows[point].setKey(kv.getRow());
							rows[point].setkeyString(CommonUtil.bytes2String(kv.getRow(), rowkeyType));
							rows[point].addColumn(col);
							point++;
						}
						offset+=length;
					}
				}
				
				if (rows != null && rows.length > 0) {
					for (int i=0;i<rows.length;i++) {
						queue.add(rows[i]);
					}
				}
				
				if (!hasMore) break;
			}
		}
		return queue.poll();
	}

	private Map<String, KeyValue> createMap(List<KeyValue> result) {
		Map<String, KeyValue> maps = new HashMap<String, KeyValue>();
		Iterator<KeyValue> ikv = result.iterator();
		
		while (ikv.hasNext()) {
			KeyValue kv = ikv.next();
			String name = Bytes.toString(kv.getFamily())+":"+
				Bytes.toString(kv.getQualifier());
			maps.put(name, kv);
		}
		return maps;
	}	
	
}
