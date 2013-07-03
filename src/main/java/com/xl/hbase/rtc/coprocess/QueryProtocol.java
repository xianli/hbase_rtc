package com.xl.hbase.rtc.coprocess;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import com.jd.bi.odp.data.hbase.reader.ColumnMeta;
import com.xl.hbase.rtc.expression.AggSelect;
import com.xl.hbase.rtc.expression.GroupBy;
import com.xl.hbase.rtc.expression.Having;
import com.xl.hbase.rtc.expression.OrderBy;
import com.xl.hbase.rtc.expression.Select;
import com.xl.hbase.rtc.expression.Where;
import com.xl.hbase.rtc.model.MetaData;
import com.xl.hbase.rtc.model.ResultSet;

/**
 * @author jiangxl
 */
public interface QueryProtocol extends CoprocessorProtocol {
	

	public ResultSet rangeQuery(byte[] startRow, 
			byte[] endRow, 
			List<ColumnMeta> columns,
			String rowkeyType) ;
	
	public ResultSet simpleQuery( 
			byte[] start,
			byte[] end,
			Select selectCol, 
			String fromTable,
			Where whereCondition,
			List<OrderBy> orderbys,
			int top, 
			MetaData meta) throws IOException;
	
	public ResultSet aggregateQuery(
			byte[] start,
			byte[] end,
			AggSelect aggSelect,
			String fromTable, 
			Where whereCondtion, 
			GroupBy groupbys ,
			Having having, 
			List<OrderBy> orderbys,
			MetaData meta) throws IOException ;
}
