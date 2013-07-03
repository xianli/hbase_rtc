package com.xl.hbase.rtc.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.jd.bi.odp.data.hbase.reader.ColumnMeta;
import com.jd.bi.odp.data.hbase.reader.HBaseConfig;
import com.jd.bi.odp.data.hbase.reader.MetaConfig;
import com.xl.hbase.rtc.coprocess.QueryImplementation;
import com.xl.hbase.rtc.coprocess.QueryProtocol;
import com.xl.hbase.rtc.expression.AggSelect;
import com.xl.hbase.rtc.expression.Aggregate;
import com.xl.hbase.rtc.expression.GroupBy;
import com.xl.hbase.rtc.expression.Having;
import com.xl.hbase.rtc.expression.OrderBy;
import com.xl.hbase.rtc.expression.Select;
import com.xl.hbase.rtc.expression.Where;
import com.xl.hbase.rtc.model.Column;
import com.xl.hbase.rtc.model.MetaData;
import com.xl.hbase.rtc.model.ResultSet;
import com.xl.hbase.rtc.model.Row;
import com.xl.hbase.rtc.reader.IReader;
import com.xl.hbase.rtc.reader.ResultScannerReader;
import com.xl.hbase.rtc.util.Matcher;
import com.xl.hbase.rtc.util.Parser;

/**
 * HBase操作类，提供hbase查询，表操作，添加coprocessor，取表元数据的方法。
 * <p>
 * 查询方法主要有get, scan, simpleQuery, aggregateQuery; get, scan是对hbase提供的Get,
 * Scan的封装；simpleQuery用于带排序，多种过滤条件的查询。aggregateQuery用于带有聚合函数的查询。
 * 
 * 
 * @author jiangxl
 */
public class Client {

	private final Logger LOG = Logger.getLogger(Client.class);
	public static final String SCF = "d";
	public static final byte[] CF = Bytes.toBytes(SCF);
	public static final int NOTOP = -1;
	private Configuration conf;

	private HTablePool pool;

	private static Client CNT;

	private Client(String zookeeperhost) {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", zookeeperhost);
		conf.set("hbase.client.retries.number", "3");

		pool = new HTablePool(conf, 20);
	}

	private Client() {
		conf = HBaseConfiguration.create();
		Properties props = HBaseConfig.getProperties();
		Set<?> keys = props.keySet();
		Iterator<?> it = keys.iterator();
		while (it.hasNext()) {
			String key = (String) it.next();
			if (key.startsWith("hbase.")) {
				// only set hbase related properties.
				conf.set((String) key, (String) props.get(key));
			}
		}
		// conf.set("hbase.zookeeper.quorum", zookeeperhost);
		// conf.set("hbase.client.retries.number", "3");
		pool = new HTablePool(conf, 20);
	}

	public Configuration getConf() {
		return conf;
	}

	public HTablePool getPool() {
		return pool;
	}

	/**
	 * one client for a java app
	 * 
	 * @param zookeeperhost
	 * @return
	 */
	public static Client instance(final String zookeeperhost) {
		if (CNT == null)
			CNT = new Client(zookeeperhost);
		return CNT;
	}

	public static Client instance() {
		if (CNT == null)
			CNT = new Client();
		return CNT;
	}

	public boolean createTable(final String tableName,
			final String defaultColFam) {
		return createTable(tableName, defaultColFam, 1);
	}

	public boolean createTable(final String tableName,
			final String defaultColFam, final int maxVersion) {
		try {
			// first check if table exists
			HBaseAdmin admin = new HBaseAdmin(conf);
			if (admin.tableExists(tableName)) {
				LOG.error("cannot create table as table already exists");
				return false;
			}
			HTableDescriptor dscp = new HTableDescriptor(tableName);
			HColumnDescriptor cfdscp = new HColumnDescriptor(defaultColFam);
			cfdscp.setMaxVersions(maxVersion);
			dscp.addFamily(cfdscp);
			admin.createTable(dscp);
			return (admin.tableExists(tableName));
		} catch (MasterNotRunningException e) {
			LOG.error(e.getMessage(), e);
		} catch (ZooKeeperConnectionException e) {
			LOG.error(e.getMessage(), e);
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		return false;
	}

	public boolean deleteTable(final String tableName) {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			if (admin.isTableEnabled(tableName))
				admin.disableTable(tableName);
			admin.deleteTable(tableName);
			return true;
		} catch (MasterNotRunningException e) {
			LOG.error(e.getMessage(), e);
		} catch (ZooKeeperConnectionException e) {
			LOG.error(e.getMessage(), e);
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		return false;
	}

	public boolean addCoprocessor(final String tableName,
			final String coprocessClazz) {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			if (admin.isTableEnabled(Bytes.toBytes(tableName)))
				admin.disableTable(Bytes.toBytes(tableName));
			HTableDescriptor desp = admin.getTableDescriptor(Bytes
					.toBytes(tableName));
			desp.addCoprocessor(coprocessClazz);
			admin.modifyTable(Bytes.toBytes(tableName), desp);
			admin.enableTable(Bytes.toBytes(tableName));
			return true;
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public boolean enableTable(final String tableName) {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			if (!admin.isTableEnabled(tableName)) {
				admin.enableTable(tableName);
				return true;
			}
		} catch (MasterNotRunningException e) {
			LOG.error(e.getMessage(), e);
		} catch (ZooKeeperConnectionException e) {
			LOG.error(e.getMessage(), e);
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		return false;
	}

	/**
	 * @see Client#get(String, byte[], List, Select)
	 * @param tableName
	 * @param rowKey
	 * @return
	 */
	public ResultSet get(final String tableName, final byte[] rowKey) {
		return get(tableName, rowKey, null, null);
	}

	/**
	 * @see Client#get(String, List, List, Select)
	 * @param tableName
	 * @param keys
	 * @return
	 */
	public ResultSet get(final String tableName, final List<byte[]> keys) {
		return get(tableName, keys, null, null);
	}

	/**
	 * 取一个row key对应的数据
	 * 
	 * @param tableName
	 *            查询的表
	 * @param keys
	 *            row key
	 * @param choose
	 *            单元格中的哪几个，跟hbasemodel.xml中的“choose”属性一样。一般可设为null
	 * @param select
	 *            要取的列；取全部列则设为null
	 * @return
	 */
	public ResultSet get(final String tableName, final byte[] rowKey,
			final List<Integer> choose, final Select select) {
		HTable table = (HTable) pool.getTable(tableName);
		ResultSet rs = new ResultSet();
		Get get = new Get(rowKey);
		try {
			Result result = table.get(get);
			MetaData meta = getMetaInfoFromCM(tableName);
			List<String> columns = null;
			if (select != null)
				columns = select.getColumns();
			rs.add(Parser.extractRow(result.list(), meta, choose, columns));
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return rs;
	}

	/**
	 * 
	 * 取多个row key对应的数据
	 * 
	 * @param tableName
	 *            查询的表
	 * @param keys
	 *            row key 列表
	 * @param choose
	 *            单元格中的哪几个，跟hbasemodel.xml中的“choose”属性一样。一般可设为null
	 * @param select
	 *            要取的列；取全部列则设为null
	 * @return
	 */
	public ResultSet get(final String tableName, final List<byte[]> keys,
			final List<Integer> choose, final Select select) {
		List<Get> gets = new ArrayList<Get>();
		for (int i = 0; i < keys.size(); ++i) {
			gets.add(new Get(keys.get(i)));
		}
		HTable table = (HTable) pool.getTable(tableName);
		ResultSet rs = new ResultSet();
		try {
			Result[] result = table.get(gets);
			MetaData meta = getMetaInfoFromCM(tableName);
			List<String> columns = null;
			if (select != null)
				columns = select.getColumns();
			for (int i = 0; i < result.length; i++) {
				rs.add(Parser.extractRow(result[i].list(), meta, choose,
						columns));
			}
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return rs;
	}

	/**
	 * 
	 * @param tableName
	 * @param startKey
	 * @param endKey
	 * @return
	 */
	public ResultSet scan(final String tableName, final byte[] startKey,
			final byte[] endKey) {
		return scan(tableName, startKey, endKey);
	}

	/**
	 * 取某一范围的row key对应的数据
	 * 
	 * @param tableName
	 *            查询的表
	 * @param startKey
	 *            开始key
	 * @param endKey
	 *            结束key
	 * @param choose
	 *            单元格中的哪几个，跟hbasemodel.xml中的“choose”属性一样。一般可设为null
	 * @param select
	 *            要取的列；取全部列则设为null
	 * @return
	 */
	public ResultSet scan(final String tableName, final byte[] startKey,
			final byte[] endKey, List<Integer> choose, Select select) {
		Scan scan = new Scan();
		if (startKey != null)
			scan.setStartRow(startKey);
		if (endKey != null)
			scan.setStopRow(endKey);
		HTable table = (HTable) pool.getTable(tableName);
		ResultSet rs = new ResultSet();
		try {
			int count = 0;
			ResultScanner scanner = table.getScanner(scan);
			Iterator<Result> ir = scanner.iterator();
			MetaData meta = getMetaInfoFromCM(tableName);
			List<String> columns = null;
			if (select != null)
				columns = select.getColumns();
			while (ir.hasNext()) {
				Result result = ir.next();
				rs.add(Parser.extractRow(result.list(), meta, choose, columns));
				if (count++ % 100 == 0)
					LOG.info(count + ",");
				if (count > 1000)
					break;
			}

			scanner.close();
			table.close();
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		return rs;
	}

	public boolean delete(String tableName, byte[] rowkey) {
		try {
			HTable table = (HTable) pool.getTable(tableName);
			Delete delete = new Delete(rowkey);
			table.delete(delete);
			table.close();
			return true;
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		return false;
	}

	public boolean put(final String tableName, final byte[] rowkey,
			final byte[] cf, final Map<byte[], byte[]> keyvalues) {
		HTable table = (HTable) pool.getTable(tableName);
		Put put = new Put(rowkey);
		Iterator<byte[]> keyIt = keyvalues.keySet().iterator();
		while (keyIt.hasNext()) {
			byte[] col = keyIt.next();
			put.add(cf, col, keyvalues.get(col));
		}
		try {
			table.put(put);
			table.close();
			return true;
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		return false;
	}

	public boolean delete(final String tableName, final List<byte[]> keys) {
		try {
			HTable table = (HTable) pool.getTable(tableName);
			List<Delete> dl = new ArrayList<Delete>();
			Iterator<byte[]> ikeys = keys.iterator();
			while (ikeys.hasNext()) {
				dl.add(new Delete(ikeys.next()));
			}
			table.delete(dl);
			table.close();
			return true;
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		return false;
	}

	public ResultSet rangeQuery(final byte[] startRow, final byte[] endRow,
			final List<ColumnMeta> columns, final String tableName)
			throws Throwable {
		class Accumulator implements Batch.Callback<ResultSet> {
			private ResultSet finalRS = null;

			public Accumulator() {
				finalRS = new ResultSet();
			}

			@Override
			public void update(byte[] region, byte[] row, ResultSet tempRS) {

				if (tempRS == null)
					return;

				if (tempRS.size() == 1) // normally, it only has one row in
										// tempRS
					finalRS.accumulate(tempRS.get(0));
				else
					LOG.warn("result set returned has " + tempRS.size()
							+ " rows");
			}

			public ResultSet getResult() {
				return finalRS;
			}
		}

		final String rowkeyType = MetaConfig.getRowKeyType(tableName);

		HTable table = (HTable) pool.getTable(tableName);
		Accumulator accumulator = new Accumulator();
		table.coprocessorExec(QueryProtocol.class, startRow, endRow,
				new Batch.Call<QueryProtocol, ResultSet>() {
					@Override
					public ResultSet call(QueryProtocol instance)
							throws IOException {
						return instance.rangeQuery(startRow, endRow, columns,
								rowkeyType);
					}
				}, accumulator);

		ResultSet result = accumulator.getResult();

		doAverageAndCaculation(result, null);

		table.close();
		return result;
	}

	/**
	 * 测试方法，所有的计算发生在本地；没有调用coprocessor；
	 * 
	 * @throws IOException
	 */
	public ResultSet simpleQueryStub(final byte[] start, final byte[] end,
			final Select selectCol, final String fromTable,
			final Where whereCondition, final List<OrderBy> orderbys,
			final int top) throws IOException {
		final MetaData meta = getMetaInfoFromCM(fromTable);
		if (!validate(selectCol, null, whereCondition, null, null, orderbys,
				meta)) {
			LOG.error("vadliation failed");
			return null;
		}

		ResultSet rs = new ResultSet();
		ResultScanner scanner = null;
		try {
			Scan scan = new Scan();
			scan.setMaxVersions(1);
			scan.setStartRow(start);
			scan.setStopRow(end);
			// prepare scan object
			List<String> colNames = Parser.extractColumnNames(selectCol, null,
					whereCondition, null, null, orderbys);
			for (int i = 0; i < colNames.size(); ++i) {
				String tmp[] = colNames.get(i).split(":");
				scan.addColumn(Bytes.toBytes(tmp[0]), Bytes.toBytes(tmp[1]));
			}
			HTable table = (HTable) pool.getTable(fromTable);
			scanner = table.getScanner(scan);

			IReader reader = new ResultScannerReader(scanner, meta, null);
			QueryImplementation qi = new QueryImplementation();
			qi.internalSimpleQuery(selectCol, fromTable, whereCondition,
					orderbys, top, meta, rs, reader);
			table.close();
		} finally {
			if (scanner != null)
				scanner.close();
		}
		return rs;
	}

	/**
	 * 此方法用于带有orderby或多种过滤条件的查询。参数的意义与SQL关键字类似，参数对象须用 ExpressionFactory中的静态方法生成。
	 * 
	 * @param start
	 * @param end
	 * @param selectCol
	 * @param fromTable
	 * @param whereCondition
	 * @param orderbys
	 * @param top
	 *            取前几条记录， 如果取全部，请设为<0
	 * @return
	 * @throws Throwable
	 */
	public ResultSet simpleQuery(final byte[] start, final byte[] end,
			final Select selectCol, final String fromTable,
			final Where whereCondition, final List<OrderBy> orderbys,
			final int top) throws Throwable {

		final MetaData meta = getMetaInfoFromCM(fromTable);
		if (!validate(selectCol, null, whereCondition, null, null, orderbys,
				meta)) {
			LOG.error("vadliation failed");
			return null;
		}

		class Merger implements Batch.Callback<ResultSet> {
			private ResultSet finalRS = null;

			public Merger() {
				finalRS = new ResultSet();
			}

			@Override
			public void update(byte[] region, byte[] row, ResultSet tempRS) {
				if (tempRS != null && tempRS.size() > 0)
					finalRS.merge(tempRS, orderbys);
			}

			public ResultSet getResult() {
				return finalRS;
			}
		}
		HTable table = (HTable) pool.getTable(fromTable);
		Merger merger = new Merger();
		table.coprocessorExec(QueryProtocol.class, start, end,
				new Batch.Call<QueryProtocol, ResultSet>() {
					@Override
					public ResultSet call(QueryProtocol instance)
							throws IOException {
						return instance.simpleQuery(start, end, selectCol,
								fromTable, whereCondition, orderbys, top, meta);
					}
				}, merger);
		table.close();
		return merger.getResult().getTop(top);
	}

	/**
	 * 测试方法，所有的计算发生在本地；没有调用coprocessor；
	 * 
	 * @return
	 * @throws IOException
	 */
	public ResultSet aggregateQueryStub(final byte[] start, final byte[] end,
			final AggSelect aggSelect, final String fromTable,
			final Where whereCondition, // where
			final GroupBy groupbys, // group by
			final Having having, final List<OrderBy> orderbys, final int top)
			throws IOException {
		final MetaData meta = getMetaInfo(fromTable);
		if (!validate(null, aggSelect, whereCondition, groupbys, having,
				orderbys, meta)) {
			LOG.error("vadliation failed");
			return null;
		}

		ResultSet rs = new ResultSet();
		ResultScanner scanner = null;
		try {
			Scan scan = new Scan();
			scan.setMaxVersions(1);
			scan.setStartRow(start);
			scan.setStopRow(end);
			// prepare scan object
			List<String> colNames = Parser.extractColumnNames(null, aggSelect,
					whereCondition, groupbys, having, orderbys);

			for (int i = 0; i < colNames.size(); ++i) {
				String tmp[] = colNames.get(i).split(":");
				scan.addColumn(Bytes.toBytes(tmp[0]), Bytes.toBytes(tmp[1]));
			}

			HTable table = (HTable) pool.getTable(fromTable);
			scanner = table.getScanner(scan);

			IReader reader = new ResultScannerReader(scanner, meta, null);
			QueryImplementation qi = new QueryImplementation();
			qi.internalAggQuery(aggSelect.getColumns(), fromTable,
					whereCondition, groupbys, having, orderbys, meta, rs,
					reader);
			table.close();
			doAverageAndCaculation(rs, aggSelect);
			rs = doHavingAndOrderBy(rs, having, orderbys, top);
		} finally {
			if (scanner != null)
				scanner.close();
		}
		return rs;
	}

	/**
	 * 参数的意义与SQL关键字类似。 此方法用于带有聚合函数(sum, avg, count, max, min)的查询，
	 * 参数对象须用ExpressionFactory中的静态方法生成。
	 * 
	 * @param start
	 * @param end
	 * @param aggSelect
	 * @param fromTable
	 * @param whereCondition
	 * @param groupbys
	 * @param having
	 * @param orderbys
	 * @param top
	 *            取前几条记录， 如果取全部，请设为<0
	 * @return
	 * @throws Throwable
	 */
	public ResultSet aggregateQuery(final byte[] start, final byte[] end,
			final AggSelect aggSelect, final String fromTable,
			final Where whereCondition, // where
			final GroupBy groupbys, // group by
			final Having having, final List<OrderBy> orderbys, final int top)
			throws Throwable {
		final MetaData meta = getMetaInfoFromCM(fromTable);
		if (!validate(null, aggSelect, whereCondition, groupbys, having,
				orderbys, meta)) {
			LOG.error("vadliation failed");
			return null;
		}

		class Accumulator implements Batch.Callback<ResultSet> {

			private ResultSet finalRS;

			public Accumulator() {
				finalRS = new ResultSet();
			}

			@Override
			public void update(byte[] region, byte[] row, ResultSet tempRS) {
				for (int i = 0; i < tempRS.size(); i++)
					finalRS.accumulate(tempRS.get(i));
			}

			public ResultSet getResult() {
				return finalRS;
			}
		}

		if (!validate(null, aggSelect, whereCondition, groupbys, having,
				orderbys, meta)) {
			LOG.error("vadliation failed");
			return null;
		}

		HTable table = (HTable) pool.getTable(fromTable);
		Accumulator accumulator = new Accumulator();

		table.coprocessorExec(QueryProtocol.class, start, end,
				new Batch.Call<QueryProtocol, ResultSet>() {
					@Override
					public ResultSet call(QueryProtocol instance)
							throws IOException {
						return instance.aggregateQuery(start, end, aggSelect,
								fromTable, whereCondition, groupbys, having,
								orderbys, meta);
					}
				}, accumulator);

		ResultSet result = accumulator.getResult();
		table.close();

		doAverageAndCaculation(result, aggSelect);
		// handle having
		return doHavingAndOrderBy(result, having, orderbys, top);
	}

	private ResultSet doHavingAndOrderBy(ResultSet result, Having having,
			List<OrderBy> orderbys, int top) {
		ResultSet finalRS = null;
		if (orderbys == null && having == null)
			finalRS = result.getTop(top);
		else if (orderbys == null && having != null) {
			finalRS = new ResultSet();
			for (int i = 0; i < result.size(); i++) {
				if (Matcher.accept(having, result.get(i)))
					finalRS.add(result.get(i));
				if (top > 0 && finalRS.size() >= top)
					break;
			}
		} else if (orderbys != null && having == null) {
			finalRS = new ResultSet();
			for (int i = 0; i < result.size(); i++) {
				finalRS.add(result.get(i), orderbys);
			}
			finalRS = finalRS.getTop(top);
		} else if (orderbys != null && having != null) {
			finalRS = new ResultSet();
			for (int i = 0; i < result.size(); i++) {
				if (Matcher.accept(having, result.get(i)))
					finalRS.add(result.get(i), orderbys);
			}
			// get top elements
			finalRS = finalRS.getTop(top);
		}
		return finalRS;
	}

	public void doAverageAndCaculation(ResultSet rs, AggSelect aggSelect) {
		// loop is used to control if scan the whole result set. if no average
		// column or
		// calculation then no need.
		boolean loop = false;
		for (int i = 0; i < rs.size(); i++) {
			Row row = rs.get(i);
			List<String> aggc = row.getAggColumns();
			for (int j = 0; j < aggc.size(); j++) {
				if (aggc.get(j).startsWith(Aggregate.PREFIX_AVG)) {
					loop = true;
					byte[] avgValue = null;
					Column sumc = row.getColumn(Aggregate.PREFIX_SUM
							+ aggc.get(j).substring(
									Aggregate.PREFIX_AVG.length()));
					Column countc = row.getColumn(Aggregate.PREFIX_COUNT);
					if (sumc != null && countc != null)
						avgValue = sumc.divide(countc);
					if (avgValue != null) {
						Column avgc = row.getColumn(aggc.get(j));
						avgc.setValue(avgValue);
						avgc.setType(Column.DataType.DOUBLE);
					}
				}
			}

			if (aggSelect != null)
				for (int j = 0; j < aggSelect.getCaculation().size(); ++j) {
					loop = true;
					int pos = aggSelect.getCaculation().get(j);
					row.doCaculation(aggSelect.getColumn(pos).getColName());
				}

			if (!loop)
				break;
		}
	}

	private boolean validate(final Select sel, final AggSelect aggsel,
			final Where whereCondition, final GroupBy groupbys,
			final Having havings, final List<OrderBy> orderbys,
			final MetaData meta) {
		List<String> colNames = Parser.extractColumnNames(sel, aggsel,
				whereCondition, groupbys, havings, orderbys);
		List<String> exists = meta.getColumns();
		boolean pass = true;
		// check if all columns exist in the table
		for (int i = 0; i < exists.size(); i++) {
			for (int j = 0; j < colNames.size(); ++j) {
				if (!exists.contains(colNames.get(j))) {
					pass = false;
					LOG.error("column not exists " + colNames.get(j));
				}
			}
		}

		if (sel == null && orderbys != null) {
			for (int i = 0; i < orderbys.size(); ++i) {
				String odn = orderbys.get(i).getColName();
				if (odn != null) {
					LOG.warn("order by " + odn
							+ " doesn't make sense in aggreate query");
				}
			}
		}
		return pass;
	}

	public void addMetaInfo(MetaData md) {
		HTable tblMeta = (HTable) pool.getTable(MetaData.TBL_META);
		Put put = new Put(Bytes.toBytes(md.getTableName()));
		// save row key type
		put.add(CF, Bytes.toBytes(MetaData.RKT_NAME), MetaData.Type.COL_TYPE,
				Bytes.toBytes(md.getRowkeyType()));
		// save columns
		List<String> cols = md.getColumns();
		for (int i = 0; i < cols.size(); ++i) {
			String col = cols.get(i);
			if (md.getColType(col) != null)
				put.add(CF, Bytes.toBytes(col), MetaData.Type.COL_TYPE,
						Bytes.toBytes(md.getColType(col)));
			if (md.isIndexed(col) != null)
				put.add(CF, Bytes.toBytes(col), MetaData.Type.HAS_IDX,
						Bytes.toBytes(md.isIndexed(col)));
		}
		try {
			tblMeta.put(put);
			tblMeta.close();
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
	}

	/**
	 * @param tbl
	 * @param tableName
	 *            row key
	 * @param fullColName
	 *            qualified name
	 * @param colType
	 * @param isIndex
	 */
	public void changeMetaInfo(String tableName, String fullColName, long type,
			byte[] value) {
		HTable tblMeta = (HTable) pool.getTable(MetaData.TBL_META);
		Put put = new Put(Bytes.toBytes(tableName));
		byte[] bname = Bytes.toBytes(fullColName);
		put.add(CF, bname, type, value);
		try {
			tblMeta.put(put);
			tblMeta.close();
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
	}

	public void delMetaInfo(String tableName) {
		HTable tblMeta = (HTable) pool.getTable(MetaData.TBL_META);
		byte[] rowKey = Bytes.toBytes(tableName);
		Delete del = new Delete(rowKey);
		try {
			tblMeta.delete(del);
			tblMeta.close();
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
	}
	/**
	 * 从XML配置中读取表的元信息
	 * @param tableName
	 * @return
	 */
	public MetaData getMetaInfoFromCM(String tableName) {
		List<ColumnMeta> metas = MetaConfig.getTableMeta(tableName);
		return MetaData.decorate(tableName, metas);
	}
	
	public MetaData getMetaInfo(String tableName) {
		Get getTableMetaData = new Get(Bytes.toBytes(tableName));
		// get all columns and their type
		try {
			HTable tblMeta = (HTable) pool.getTable(MetaData.TBL_META);
			Result result = tblMeta.get(getTableMetaData);
			if (result.isEmpty())
				return null;

			MetaData mt = new MetaData();
			mt.setTableName(tableName);

			NavigableMap<byte[], NavigableMap<Long, byte[]>> nmap = result
					.getMap().get(CF);

			Iterator<byte[]> iks = nmap.keySet().iterator();

			while (iks.hasNext()) {
				byte[] column = iks.next(); // it contains the prefix "d:"

				NavigableMap<Long, byte[]> versionValues = nmap.get(column);
				// get 1st version: data type
				byte[] btype = versionValues.get(MetaData.Type.COL_TYPE);
				// get 2nd version: if has index
				byte[] bindex = versionValues.get(MetaData.Type.HAS_IDX);

				String sColumn = Bytes.toString(column);

				// if key is MetaTable.RKT_NAME
				if (MetaData.RKT_NAME.equals(sColumn)) {
					mt.setRowkeyType(Bytes.toString(btype));
				} else {
					if (btype != null)
						mt.addColType(sColumn, Bytes.toString(btype));
					if (bindex != null)
						mt.addIsIndexed(sColumn, Bytes.toBoolean(bindex));
				}
			}
			tblMeta.close();
			return mt;
		} catch (IOException e) {
			LOG.error(e.getMessage(), e);
		}
		return null;
	}
}
