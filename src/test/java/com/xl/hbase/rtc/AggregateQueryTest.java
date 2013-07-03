package com.xl.hbase.rtc;

import static com.xl.hbase.rtc.expression.ExpressionFactory.aggcal;
import static com.xl.hbase.rtc.expression.ExpressionFactory.as;
import static com.xl.hbase.rtc.expression.ExpressionFactory.asc;
import static com.xl.hbase.rtc.expression.ExpressionFactory.aslist;
import static com.xl.hbase.rtc.expression.ExpressionFactory.avg;
import static com.xl.hbase.rtc.expression.ExpressionFactory.eq;
import static com.xl.hbase.rtc.expression.ExpressionFactory.groupby;
import static com.xl.hbase.rtc.expression.ExpressionFactory.having;
import static com.xl.hbase.rtc.expression.ExpressionFactory.lt;
import static com.xl.hbase.rtc.expression.ExpressionFactory.out;
import static com.xl.hbase.rtc.expression.ExpressionFactory.select;
import static com.xl.hbase.rtc.expression.ExpressionFactory.sum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import com.jd.bi.odp.data.hbase.transfomer.Transformer;
import com.xl.hbase.rtc.coprocess.QueryImplementation;
import com.xl.hbase.rtc.expression.AggSelect;
import com.xl.hbase.rtc.expression.GroupBy;
import com.xl.hbase.rtc.expression.Having;
import com.xl.hbase.rtc.expression.OrderBy;
import com.xl.hbase.rtc.expression.Where;
import com.xl.hbase.rtc.model.MetaData;
import com.xl.hbase.rtc.model.ResultSet;
import com.xl.hbase.rtc.reader.IReader;
import com.xl.hbase.rtc.reader.ResultScannerReader;
import com.xl.hbase.rtc.service.Client;
import com.xl.hbase.rtc.util.Parser;

public class AggregateQueryTest {
	
	Client cnt;
	String tablename = "HB_TEAM_SALE";
	String pageName = "getTeamSaleTotal";
	@Before public void init() {
		cnt = Client.instance("vmdev40");
	}
	
	@Test public void testAggInConTime() throws Throwable {
		String tableName = "HB_SHOP_CONS_DTL";
		AggSelect aggSelect = select(as(sum("d:ConsNum"),"ConsNum"), as(sum("d:RespNum"),"RespNum"),
				as(sum("d:NoRespNum"),"NoRespNum"),
				as(aggcal("sum(d:ConsNum)/sum(d:NoRespNum)"),"NotRespRate"),as(out("_row_1"),"Date"),as(out("_row_2"),"Area"));
		
		GroupBy groupbys = groupby("_row_1");
		ByteBuffer startRow = ByteBuffer.allocate(8);
		startRow.putInt(11001).putInt(20120722);
		ByteBuffer endRow = ByteBuffer.allocate(8);
		endRow.putInt(11001).putInt(20120725 + 1);
		
		MetaData meta = cnt.getMetaInfo(tableName);
		HTable table = new HTable(cnt.getConf(), tableName);
		Scan scan = new Scan();
		scan.setMaxVersions(1);
		scan.setStartRow(startRow.array());
		scan.setStopRow(endRow.array());
		ResultScanner sc = table.getScanner(scan);
		IReader reader = new ResultScannerReader(sc, meta, null);
		QueryImplementation qi = new QueryImplementation();
		ResultSet rs = new ResultSet();
		qi.internalAggQuery(aggSelect.getColumns(), "", null, groupbys, 
				null, null, meta, rs, reader);
		sc.close();
		
		cnt.doAverageAndCaculation(rs, aggSelect);
//		
//		ResultSet frs = cnt.doHavingAndOrderBy(rs, havings, orderbys, -1);
		
//		rs.print(System.out);
		Transformer trans=new Transformer();
		System.out.println(trans.toJSONObject(rs, aggSelect));
	}
	
	@Test public void testAggQuery() throws IOException {
		QueryImplementation qi = new QueryImplementation();
		
		AggSelect aggSelect=select(sum("d:UPV"),sum("d:CancelAmt"), avg("d:PurchaseAmt"),
				aggcal("sum(d:CancelAmt), avg(d:PurchaseAmt)" )); 
		Where whereCondition1 =  lt("_row_3", 20120617);
		Where whereCondition2 =  eq("_row_3", 20120618);
		Where whereCondition3 =  eq("_row_3", 20120619);
		GroupBy groupbys = groupby("_row_2");
		List<OrderBy> orderbys = aslist(asc(sum("d:CancelAmt")));
		Having havings = having( lt(sum("d:CancelAmt"), 6104.0));
		
		Scan scan = new Scan();
		scan.setMaxVersions(1);
		scan.setStartRow(Bytes.toBytes(10000));
		scan.setStopRow(Bytes.toBytes(10030));
		// prepare scan object
		List<String> colNames = Parser.extractColumnNames(null, aggSelect, whereCondition1,
				groupbys, null, orderbys);
		
		for (int i=0;i<colNames.size();++i) {
			System.out.println(colNames.get(i));
			String tmp[] = colNames.get(i).split(":");
			scan.addColumn(Bytes.toBytes(tmp[0]), Bytes.toBytes(tmp[1]));
		}
		
		MetaData meta = cnt.getMetaInfo("HB_TEAM_SALE");
		HTable table = new HTable(cnt.getConf(), "HB_TEAM_SALE");

		ResultSet rs = new ResultSet();
		ResultScanner sc = table.getScanner(scan);
		IReader reader = new ResultScannerReader(sc, meta, null);
		qi.internalAggQuery(aggSelect.getColumns(), "", whereCondition1, groupbys, 
				null, orderbys, meta, rs, reader);
		sc.close();
		
		ResultSet rs2 = new ResultSet();
		ResultScanner sc2 = table.getScanner(scan);
		IReader reader2 = new ResultScannerReader(sc2, meta, null);
		qi.internalAggQuery(aggSelect.getColumns(), "", whereCondition2, groupbys, 
				null, orderbys, meta, rs2, reader2);
		sc2.close();
		
		ResultSet rs3 = new ResultSet();
		ResultScanner sc3 = table.getScanner(scan);
		IReader reader3 = new ResultScannerReader(sc3, meta, null);
		qi.internalAggQuery(aggSelect.getColumns(), "", whereCondition3, groupbys, 
				null, orderbys, meta, rs3,  reader3);
		sc3.close();
		
		for (int i=0;i<rs2.size();i++)
			rs.accumulate(rs2.get(i));
		
		for (int i=0;i<rs3.size();i++)
			rs.accumulate(rs3.get(i));
		
//		cnt.doAverageAndCaculation(rs, aggSelect);
//		
//		ResultSet frs = cnt.doHavingAndOrderBy(rs, havings, orderbys, -1);
		
//		frs.print(System.out);
	}
}
