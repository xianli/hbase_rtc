package com.xl.hbase.rtc;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import com.jd.bi.odp.data.hbase.reader.ColumnMeta;
import com.jd.bi.odp.data.hbase.reader.MetaConfig;
import com.jd.bi.odp.data.hbase.transfomer.Transformer;
import com.xl.hbase.rtc.model.ResultSet;
import com.xl.hbase.rtc.service.Client;

public class QueryTest {
	Client cnt;
	String tablename = "HB_TEAM_SALE";
	String pageName = "getTeamSaleTotal";
	@Before public void init() {
		cnt = Client.instance("vmdev40");
	}
	
	@Test public void testRQMultiRegion() {
		ByteBuffer start = ByteBuffer.allocate(17);
		start.put(Bytes.toBytes(10000));
		
		ByteBuffer end = ByteBuffer.allocate(17);
		end.put(Bytes.toBytes(15000));
		
		List<ColumnMeta> metas = MetaConfig.getColumnsMeta(tablename, pageName);
		try {
			ResultSet rs = cnt.rangeQuery(start.array(), end.array(), metas, tablename);
			rs.print(System.out);
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
	
	@Test public void testRQSingleRegion() {
		ByteBuffer start = ByteBuffer.allocate(4);
		start.put(Bytes.toBytes(14000));
		
		ByteBuffer end = ByteBuffer.allocate(4);
		end.put(Bytes.toBytes(15000));
		
		List<ColumnMeta> metas = MetaConfig.getColumnsMeta(tablename, pageName);
		try {
			ResultSet rs = cnt.rangeQuery(start.array(), end.array(), metas, tablename);
			rs.print(System.out);
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
	
	@Test public void testRQOneRow() {
		ByteBuffer start = ByteBuffer.allocate(17);
		byte sku = 0;
		start.put(Bytes.toBytes(10017));
		start.put(sku);
		start.put(Bytes.toBytes(1000248509l));
		start.put(Bytes.toBytes(20120607));
		
		ByteBuffer end = ByteBuffer.allocate(17);
		end.put(Bytes.toBytes(10017));
		end.put(sku);
		end.put(Bytes.toBytes(1000248509l));
		end.put(Bytes.toBytes(20120607));
		
		List<ColumnMeta> metas = MetaConfig.getColumnsMeta(tablename, pageName);
		try {
			ResultSet rs = cnt.rangeQuery(start.array(), end.array(), metas, tablename);
			rs.print(System.out);
			Transformer trans = new Transformer();
			System.out.println(trans.toJSONObject(metas, rs).toString());
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
	
}
