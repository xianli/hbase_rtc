package com.xl.hbase.rtc.reader;

import java.io.IOException;

import com.xl.hbase.rtc.model.Row;

public interface IReader {
	public Row next() throws IOException;
}
