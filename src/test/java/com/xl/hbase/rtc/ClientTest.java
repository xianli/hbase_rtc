package com.xl.hbase.rtc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import com.jd.bi.odp.data.hbase.inf.CommonUtil;
import com.jd.bi.odp.data.hbase.reader.MetaConfig;
import com.xl.hbase.rtc.coprocess.QueryProtocol;
import com.xl.hbase.rtc.expression.AggSelect;
import com.xl.hbase.rtc.expression.GroupBy;
import com.xl.hbase.rtc.expression.Having;
import com.xl.hbase.rtc.expression.OrderBy;
import com.xl.hbase.rtc.expression.Select;
import com.xl.hbase.rtc.expression.Where;
import com.xl.hbase.rtc.model.MetaData;
import com.xl.hbase.rtc.model.ResultSet;
import com.xl.hbase.rtc.service.Client;

import static com.xl.hbase.rtc.expression.ExpressionFactory.*;

public class ClientTest {
	Client cnt;
	private String tableName;

	@Before
	public void init() {
		cnt = Client.instance("vmdev40");
		tableName = "slave0";
	}

	@Test
	public void testCreate() {
		cnt.createTable(MetaData.TBL_META, "d");
	}

	@Test
	public void testCreateTableWithKeyRange() {
		try {
			HBaseAdmin admin = new HBaseAdmin(cnt.getConf());
			byte[][] regions = new byte[][] { Bytes.toBytes(100000l),
					Bytes.toBytes(200000l), Bytes.toBytes(300000l),
					Bytes.toBytes(400000l), Bytes.toBytes(500000l),
					Bytes.toBytes(600000l), Bytes.toBytes(700000l),
					Bytes.toBytes(800000l), Bytes.toBytes(900000l) };
			HTableDescriptor dscp = new HTableDescriptor(tableName);
			dscp.addFamily(new HColumnDescriptor(Bytes.toBytes("d")));
			admin.createTable(dscp, regions);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testDeleteTable() {
		cnt.deleteTable(tableName);
	}

	@Test
	public void testPutWithKVMap() {
		Map<byte[], byte[]> values = new HashMap<byte[], byte[]>();
		values.put(Bytes.toBytes("a"), Bytes.toBytes("test"));
		values.put(Bytes.toBytes("b"), Bytes.toBytes("test"));
		values.put(Bytes.toBytes("c"), Bytes.toBytes("test"));
		for (int i = 0; i < 10000; i++) {
			cnt.put("SPLIT_TEST", Bytes.toBytes(i), Bytes.toBytes("d"), values);
			System.out.print(i + ",");
		}
	}

	@Test
	public void testAdminFlush() {
		HBaseAdmin admin;
		try {
			admin = new HBaseAdmin(cnt.getConf());
			admin.flush(MetaData.TBL_META);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testPutMetaData() {
		String[] names = MetaConfig.getTableNames();
		for (int i = 0; i < names.length; ++i) {
			MetaData md = MetaData.decorate(names[i],
					MetaConfig.getTableMeta(names[i]));
			cnt.addMetaInfo(md);
		}
	}

	@Test
	public void testChangeMetaData() {
		cnt.changeMetaInfo("HB_DIM_SHOP_PRODUCT", "d" + MetaData.RKT_NAME,
				MetaData.Type.COL_TYPE, Bytes.toBytes("int,byte"));
	}

	@Test
	public void testGetMetaData2() {
		MetaData md = cnt.getMetaInfo("HB_DIM_SHOP_PRODUCT");
		List<String> names = md.getColumns();
		for (int i = 0; i < names.size(); ++i) {
			System.out.println(names.get(i) + "|" + md.getColType(names.get(i))
					+ "|");
		}
	}

	@Test
	public void testDelMetaData() {
		cnt.delMetaInfo("HB_PRO_RTN_EX");
	}

	@Test
	public void testGet() {
		List<byte[]> rowkeys = new ArrayList<byte[]>();
		rowkeys.add(Bytes.add(Bytes.toBytes(10031), Bytes.toBytes(20120531)));
		ResultSet rs = cnt.get("HB_SHOP_DAILY_DETAIL",
				Bytes.add(Bytes.toBytes(10031), Bytes.toBytes(20120531)));
		rs.print(System.out);
	}

	@Test
	public void testScan() {
		ResultSet rs = cnt.scan("HB_TEAM_SALE", null, null);
		rs.print(System.out);
	}

	@Test
	public void testDelete() {
		cnt.delete(tableName, Bytes.toBytes(1000l));
	}

	@Test
	public void testScanRaw() {
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(1l));
		scan.setStopRow(Bytes.toBytes(1000l));
		HTable table = (HTable) cnt.getPool().getTable(tableName);
		try {
			long start = System.currentTimeMillis();
			ResultScanner scanner = table.getScanner(scan);
			Iterator<Result> ir = scanner.iterator();
			int count = 0;
			while (ir.hasNext()) {
				Result result = ir.next();
				KeyValue[] kv = result.raw();
				if (count++ % 500 == 0)
					System.out.println(count);
			}
			System.out.println(System.currentTimeMillis() - start);
			scanner.close();
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testGetTableTimeInCP() throws IOException, Throwable {
		HTable table = (HTable) cnt.getPool().getTable(tableName);
		table.coprocessorExec(QueryProtocol.class, null, null,
				new Batch.Call<QueryProtocol, ResultSet>() {
					@Override
					public ResultSet call(QueryProtocol instance)
							throws IOException {
						return null;
					}
				}, null);
	}

	@Test
	public void testCPQuery() {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "vmdev40");
		conf.set("hbase.client.retries.number", "2");
		try {
			HTable table = new HTable(conf, "SPLIT_TEST");
			table.coprocessorExec(QueryProtocol.class, null, null,
					new Batch.Call<QueryProtocol, Object>() {
						@Override
						public Object call(QueryProtocol instance)
								throws IOException {
							return null;
						}
					});
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testAddCP() {
		cnt.addCoprocessor("HB_TEAM_SALE_NEW",
				"com.jd.hbase.rtc.coprocess.QueryImplementation");
	}

	@Test
	public void testGetData() {
		List<byte[]> gets = new ArrayList<byte[]>();
		gets.add(Bytes.add(Bytes.toBytes(10017), Bytes.toBytes(20120616)));
		gets.add(Bytes.add(Bytes.toBytes(10021), Bytes.toBytes(20120616)));
		gets.add(Bytes.add(Bytes.toBytes(10031), Bytes.toBytes(20120616)));
		gets.add(Bytes.add(Bytes.toBytes(10126), Bytes.toBytes(20120616)));
		cnt.get(tableName, gets).print(System.out);

		// 10017,0,1000002352,20120617,
		/*
		 * byte[] key = Bytes.add(Bytes.toBytes(10017),
		 * Bytes.add(Bytes.toBytes(0), Bytes.add(Bytes.toBytes(1000002352l),
		 * Bytes.toBytes(20120617)))); ResultSet rs = cnt.get("HB_TEAM_SALE",
		 * key); rs.print(System.out);
		 */
	}

	@Test
	public void testMTWrite() {
		final List<Long> toThread = new ArrayList<Long>();
		ClientTest ct = new ClientTest();
		for (long i = 0; i < 200000; i++) {
			toThread.add(i);
			if (toThread.size() > 20000) {
				WriteThread wt = ct.new WriteThread(toThread);
				wt.start();
				toThread.clear();
			}
		}
	}

	@Test
	public void testMTPutWithRawData() {
		final List<Long> toThread = new ArrayList<Long>();
		for (long i = 0; i < 20000; i++) {
			toThread.add(i);
			if (toThread.size() > 5000) {
				WriteThread wt = new WriteThread(toThread);
				wt.start();
				toThread.clear();
			}
		}
	}

	class WriteThread extends Thread {
		private List<Long> shopids;

		public WriteThread(List<Long> shopids) {
			this.shopids = new ArrayList<Long>();
			this.shopids.addAll(shopids);
		}

		@Override
		public void run() {
			HTable table = (HTable) cnt.getPool().getTable(tableName);
			Random rand = new Random();
			final byte[] d = Bytes.toBytes("d");
			final byte[] pv = Bytes.toBytes("PV");
			final byte[] uv = Bytes.toBytes("UV");
			final byte[] upv = Bytes.toBytes("UPV");
			try {
				table.setAutoFlush(false);
				for (int i = 0; i < shopids.size(); i++) {
					Put put = new Put(Bytes.toBytes(shopids.get(i)));
					put.add(d, pv, 1l, Bytes.toBytes(rand.nextInt()));
					put.add(d, uv, 1l, Bytes.toBytes(rand.nextInt()));
					put.add(d, upv, 1l, Bytes.toBytes(rand.nextInt()));
					table.put(put);

					if (i % 1000 == 0)
						System.out.println("1000++");
				}
				table.flushCommits();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	@Test
	public void testSimpleQuery() {
		Select selectCol = select("d:UPV", "d:CancelAmt", "d:PurchaseAmt",
				"_row_0", "_row_1", "_row_2", "d:PurchaseOrderNum");
		Where whereCondition1 = eq("_row_3", 20120618);
		Where whereCondition2 = eq("_row_3", 20120617);
		Where whereCondition3 = eq("_row_3", 20120619);

		// or(gt("d:UPV", 100), and(lt("d:CancelAmt", 100.0d),
		// gt("d:PurchaseAmt", 19812.0d)));
		List<OrderBy> orderbys = aslist(asc("d:UPV"), asc("_row_2"));
		try {
			ResultSet rs = cnt.simpleQuery(Bytes.toBytes(10017),
					Bytes.toBytes(10030), selectCol, "HB_TEAM_SALE",
					whereCondition2, orderbys, -1);
			rs.print(System.out);
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testAggregate() {
		AggSelect aggSelect = select(sum("d:UPV"), sum("d:CancelAmt"),
				avg("d:PurchaseAmt"),
				aggcal("sum(d:CancelAmt)/avg(d:PurchaseAmt)"));
		Where whereCondition1 = lt("_row_3", 20120617);
		Where whereCondition2 = eq("_row_3", 20120618);
		Where whereCondition3 = eq("_row_3", 20120619);
		GroupBy groupbys = groupby("_row_2");
		List<OrderBy> orderbys = aslist(asc(sum("d:CancelAmt")));
		Having havings = having(lt(sum("d:CancelAmt"), 6104.0));

		try {
			ResultSet rs = cnt.aggregateQuery(Bytes.toBytes(10000),
					Bytes.toBytes(10030), aggSelect, "HB_TEAM_SALE",
					whereCondition2, groupbys, havings, orderbys, -1);
			rs.print(System.out);
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testGetEndKey() {
		byte[] end = CommonUtil.getEndKey(
				Bytes.add(Bytes.toBytes(1234), Bytes.toBytes(1235)), "int,int");
		Assert.assertEquals(CommonUtil.bytes2String(end, "int,int"),
				"1234,1236,");
		byte[] end2 = CommonUtil.getEndKey(
				Bytes.add(Bytes.toBytes(1234), Bytes.toBytes(123501l)),
				"int,long");
		Assert.assertEquals(CommonUtil.bytes2String(end2, "int,long"),
				"1234,123502,");
		byte one = 1;
		byte[] end3 = CommonUtil.getEndKey(
				Bytes.add(Bytes.toBytes(1234), new byte[] { one }), "int,byte");
		Assert.assertEquals(CommonUtil.bytes2String(end3, "int,byte"),
				"1234,2,");
	}
	
	@Test public void test() {
		byte[] b = Bytes.toBytes("StringStringString");
		System.out.println(b.length);
		System.out.print(Bytes.hashCode(b));
	}
}
