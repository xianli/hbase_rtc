package com.xl.hbase.rtc.reader;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import com.xl.hbase.rtc.model.MetaData;
import com.xl.hbase.rtc.model.Row;
import com.xl.hbase.rtc.util.Parser;

public class ResultScannerReader implements IReader {

	private LinkedList<Row> queue;
	private int bufferSize = 20;
	
	private ResultScanner scanner; 
	private MetaData meta;
	private List<Integer> selected;
	
	public ResultScannerReader(final ResultScanner scanner, 
			final MetaData meta, 
			final List<Integer> selected) {
		this.scanner = scanner;
		this.meta = meta;
		this.selected = selected;
		
		queue = new LinkedList <Row>();
	}
	
	@Override
	public Row next() throws IOException  {
		if (queue.isEmpty()) {
			while (true) { 
				if (queue.size() > bufferSize) break;
				Result result = scanner.next();
				if (result==null) break;
				Row[] rows = Parser.extractRow(result.list(), meta, selected, null);
				if (rows != null && rows.length > 0) {
					for (int i=0;i<rows.length;i++) {
						queue.add(rows[i]);
					}
				}
			}
		} 
		return queue.poll();
	}
}
