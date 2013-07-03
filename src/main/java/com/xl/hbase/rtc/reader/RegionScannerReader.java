package com.xl.hbase.rtc.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import com.xl.hbase.rtc.model.MetaData;
import com.xl.hbase.rtc.model.Row;
import com.xl.hbase.rtc.util.Parser;

public class RegionScannerReader implements IReader {
	
	private LinkedList<Row> queue;
	private int bufferSize = 20;
	
	private RegionScanner scanner;
	private MetaData meta;
	private List<Integer> selected;
	
	public RegionScannerReader(final RegionScanner scanner, 
								final MetaData meta, 
								final List<Integer> selected) {
		this.scanner = scanner;
		this.meta = meta;
		this.selected = selected;
		
		queue = new LinkedList <Row>();
	}
	
	@Override
	public Row next() throws IOException {
		if (queue.isEmpty()) {
			while (true) { 
				if (queue.size() > bufferSize) break;
				
				List<KeyValue> result = new ArrayList<KeyValue>();
				boolean hasMore=scanner.next(result);
				Row[] rows = Parser.extractRow(result, meta, selected, null);
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
}
