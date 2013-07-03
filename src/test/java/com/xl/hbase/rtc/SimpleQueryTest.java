package com.xl.hbase.rtc;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import com.jd.bi.odp.data.hbase.transfomer.Transformer;
import com.xl.hbase.rtc.coprocess.QueryImplementation;
import com.xl.hbase.rtc.expression.OrderBy;
import com.xl.hbase.rtc.expression.Select;
import com.xl.hbase.rtc.expression.Where;
import com.xl.hbase.rtc.model.MetaData;
import com.xl.hbase.rtc.model.ResultSet;
import com.xl.hbase.rtc.model.Row;
import com.xl.hbase.rtc.reader.IReader;
import com.xl.hbase.rtc.reader.ResultScannerReader;
import com.xl.hbase.rtc.service.Client;
import com.xl.hbase.rtc.util.Parser;

import static com.xl.hbase.rtc.expression.ExpressionFactory.*;

public class SimpleQueryTest {
	
	Client cnt;
	String tablename = "HB_TEAM_SALE";
	String pageName = "getTeamSaleTotal";
	@Before public void init() {
		cnt = Client.instance("vmdev40");
	}
	@Test public void testQueryInSatisfaction() throws IOException {
		String tableName = "HB_CUSS_SATIS_ANALY";
		Select colSelect = select(
				"d:CustSrvID",
				"d:VerySatisNum",
				"d:SatisNum",
				"d:OrdiNum",
				"d:NotSatisNum",
				"d:VeryNotSatisNum",
				"d:NotCommNum",
				as(cal("(d:VerySatisNum+d:SatisNum)/(d:VerySatisNum+d:SatisNum+d:OrdiNum+d:NotSatisNum+d:VeryNotSatisNum+d:NotCommNum)"), "test"));
		Scan scan = new Scan();
		scan.setMaxVersions(1);
		scan.setStartRow(Bytes.add(Bytes.toBytes(11001),Bytes.toBytes(20120729)));
		scan.setStopRow(Bytes.add(Bytes.toBytes(11001),Bytes.toBytes(20120731)));
		HTable table = new HTable(cnt.getConf(), tableName);
		ResultScanner scanner = table.getScanner(scan);
		ResultSet set1 = new ResultSet();
		MetaData meta = cnt.getMetaInfo(tableName);
		IReader reader = new ResultScannerReader(scanner, meta, null);
		QueryImplementation query = new QueryImplementation();
		
		query.internalSimpleQuery(colSelect, tableName, null, null, -1, meta, set1, reader);
		set1.print(System.out);
		Transformer trans = new Transformer();
		System.out.println( trans.toJSONObject(set1, colSelect));
	}
	
	@Test public void testInternalQuery() throws IOException {
		String tableName = "HB_CUSS_PERF_ASSES";
		Select colSelect = select("d:CustSrvID", "d:OnlineTime",
				"d:RespNum", "d:RespRate", "d:RespTimeAvg",
				"d:RespNumRatio", "d:OrdNum", "d:RespOrdRate", "d:OrdAmt",
				"d:OrdExwNum", "d:OrdExwRate", "d:OrdExwAmt",
				"d:OrdExwAvgPri");
		
		List<String> names = Parser.extractColumnNames(colSelect, null, null,  null, null, null);
		Scan scan = new Scan();
		scan.setMaxVersions(1);
		scan.setStartRow(Bytes.add(Bytes.toBytes(11001),Bytes.toBytes(20120601)));
		scan.setStopRow(Bytes.add(Bytes.toBytes(11001),Bytes.toBytes(20120701)));
		HTable table = new HTable(cnt.getConf(), tableName);
		ResultScanner scanner = table.getScanner(scan);
		ResultSet set1 = new ResultSet();
		MetaData meta = cnt.getMetaInfo(tableName);
		IReader reader = new ResultScannerReader(scanner, meta, null);
		QueryImplementation query = new QueryImplementation();
		
		query.internalSimpleQuery(colSelect, tableName, null, null, -1, meta, set1, reader);
		set1.print(System.out);
	}
	
	
	@Test public void testSQOnPerf() throws IOException {
		String tableName = "HB_CUSS_PERF_ASSES";
		Select colSelect = select("d:CustSrvID", "d:OnlineTime",
				"d:RespNum", "d:RespRate", "d:RespTimeAvg",
				"d:RespNumRatio", "d:OrdNum", "d:RespOrdRate", "d:OrdAmt",
				"d:OrdExwNum", "d:OrdExwRate", "d:OrdExwAmt",
				"d:OrdExwAvgPri");
		
		List<String> names = Parser.extractColumnNames(colSelect, null, null,  null, null, null);
		Scan scan = new Scan();
		scan.setMaxVersions(1);
		scan.setStartRow(Bytes.add(Bytes.toBytes(11001),Bytes.toBytes(20120601)));
		scan.setStopRow(Bytes.add(Bytes.toBytes(11001),Bytes.toBytes(20120701)));
		for (int i=0;i<names.size();i++) {
			String tmp[] = names.get(i).split(":");
			scan.addColumn(Bytes.toBytes(tmp[0]), Bytes.toBytes(tmp[1]));
		}
		HTable table = new HTable(cnt.getConf(), tableName);
		ResultScanner scanner = table.getScanner(scan);
		MetaData meta = cnt.getMetaInfo(tableName);
		IReader reader = new ResultScannerReader(scanner, meta, null);
		
		ResultSet set1 = new ResultSet();
		while (true) {
			Row row = reader.next();
			if (row == null) break;
//			if (Matcher.accept(whereCondition1, row))
				set1.add(row);
//			else if (Matcher.accept(whereCondition2, row)) 
//				set2.add(row, orderbys);
//			else if (Matcher.accept(whereCondition3, row)) 
//				set3.add(row, orderbys);
		}
		set1.print(System.out);
	}
	@Test public void testSimpleQueryMerger() {
		String tableName = "HB_TEAM_SALE";
		try {
			//prepare 
			HTable table = new HTable(cnt.getConf(), tableName);
			Scan scan = new Scan();
			scan.setMaxVersions(1);
			scan.setStartRow(Bytes.toBytes(10000));
			scan.setStopRow(Bytes.toBytes(21000));
			Select selectCol = select("d:UPV","d:CancelAmt",
								"d:PurchaseAmt","_row_0",
								"_row_1","_row_2", "d:PurchaseOrderNum");
			Where whereCondition1 =  eq("_row_3", 20120618);
			Where whereCondition2 =  eq("_row_3", 20120617);
			Where whereCondition3 =  eq("_row_3", 20120619);
			
			//or(gt("d:UPV", 100), and(lt("d:CancelAmt", 100.0d), gt("d:PurchaseAmt", 19812.0d)));
			List<OrderBy> orderbys = aslist(asc("d:UPV"), asc("_row_2"));
		
			List<String> names = Parser.extractColumnNames(selectCol, null, null,  null, null, orderbys);
			
			for (int i=0;i<names.size();i++) {
				String tmp[] = names.get(i).split(":");
				scan.addColumn(Bytes.toBytes(tmp[0]), Bytes.toBytes(tmp[1]));
			}
			
			ResultScanner scanner = table.getScanner(scan);
			MetaData meta = cnt.getMetaInfo(tableName);
			ResultSet set1 = new ResultSet();
			ResultSet set2 = new ResultSet();
			ResultSet set3 = new ResultSet();
			IReader reader = new ResultScannerReader(scanner, meta, null);
			while (true) {
				Row row = reader.next();
				if (row == null) break;
//				if (Matcher.accept(whereCondition1, row))
					set1.add(row);
//				else if (Matcher.accept(whereCondition2, row)) 
//					set2.add(row, orderbys);
//				else if (Matcher.accept(whereCondition3, row)) 
//					set3.add(row, orderbys);
			}
//			set1.merge(set2, orderbys);
//			set1.merge(set3, orderbys);
			set1.print(System.out);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test public void testSimpleQuery2() throws IOException {
		Select selectCol = select("d:UPV","d:CancelAmt",
				"d:PurchaseAmt","_row_0",
				"_row_1","_row_2", "d:PurchaseOrderNum", "_row_3", 
				cal("d:CancelAmt/d:PurchaseAmt"),
				as(cal("d:CancelAmt/d:PurchaseOrderNum"), "132"));
		Where whereCondition1 =  eq("_row_3", 20120607);
		Where whereCondition2 =  eq("_row_3", 20120617);
		Where whereCondition3 =  eq("_row_3", 20120606);
		List<OrderBy> orderbys = aslist(asc("d:UPV"), asc("_row_2"));
		//or(gt("d:UPV", 100), and(lt("d:CancelAmt", 100.0d), gt("d:PurchaseAmt", 19812.0d)));
//		List<OrderBy> orderbys = asList(asc("d:UPV"), asc("_row_2"));
		Scan scan = new Scan();
		scan.setMaxVersions(1);
		scan.setStartRow(Bytes.toBytes(10017));
		scan.setStopRow(Bytes.toBytes(10050));
		// prepare scan object
		List<String> colNames = Parser.extractColumnNames(selectCol, null,  whereCondition2,
				null, null, orderbys);
		
		for (int i=0;i<colNames.size();++i) {
			System.out.println(colNames.get(i));
			String tmp[] = colNames.get(i).split(":");
			scan.addColumn(Bytes.toBytes(tmp[0]), Bytes.toBytes(tmp[1]));
		}
		
		HTable table = new HTable(cnt.getConf(), "HB_TEAM_SALE");
		ResultScanner sc = table.getScanner(scan);
		ResultSet rs = new ResultSet();
		QueryImplementation qi = new QueryImplementation();
		MetaData meta = cnt.getMetaInfo("HB_TEAM_SALE");
		IReader reader = new ResultScannerReader(sc, meta, null);
		qi.internalSimpleQuery(selectCol, "", whereCondition2,  orderbys,
				300, meta, rs, reader);
		sc.close();
		rs.print(System.out);
	}
	
	
	@Test public void testWithDivide() {
		
	}
}