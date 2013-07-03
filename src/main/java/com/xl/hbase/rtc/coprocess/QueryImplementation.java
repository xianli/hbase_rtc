package com.xl.hbase.rtc.coprocess;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.jd.bi.odp.data.hbase.reader.ColumnMeta;
import com.xl.hbase.rtc.expression.AggSelect;
import com.xl.hbase.rtc.expression.Aggregate;
import com.xl.hbase.rtc.expression.Expression;
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
import com.xl.hbase.rtc.reader.RegionScannerReader;
import com.xl.hbase.rtc.util.CMReader;
import com.xl.hbase.rtc.util.Matcher;
import com.xl.hbase.rtc.util.Parser;

/**
 * @author jiangxl
 */
public class QueryImplementation extends BaseEndpointCoprocessor implements
		QueryProtocol {
	
	protected Logger LOG = Logger.getLogger(QueryImplementation.class);
	
	/**
	 * {@inheritDoc}
	 * @throws IOException 
	 */
	@Override
	public ResultSet rangeQuery(byte[] startRow, 
				byte[] endRow, 
				List<ColumnMeta> columns,
				String rowkeyTyep)  {
		RegionScanner scanner = null;
		try {
			Scan scan = new Scan();
			scan.setStartRow(startRow);
			scan.setStopRow(endRow);
			scan.setMaxVersions(1);
			scanner = ((RegionCoprocessorEnvironment) getEnvironment())
			.getRegion().getScanner(scan);
			ResultSet rs = new ResultSet();
			CMReader reader = new CMReader();
			do {
				Row currentRow = reader.next(scanner, columns, rowkeyTyep);
				if (currentRow == null) break;
				//add aggregate columns
				List<Aggregate> aggCol = new ArrayList<Aggregate>();
				
				for (int i=0;i<columns.size();i++) {
					ColumnMeta ameta = columns.get(i);
					if (ameta.getAggregate() != null && !ameta.equals("")) {
						aggCol.add(new Aggregate(ameta.getFullColName(),
								Aggregate.getTypeByName(ameta.getAggregate())));
					}
				}
				for (int i=0;i<aggCol.size();i++) {
					generateAggregate(currentRow, aggCol.get(i));
				}
				rs.accumulate(currentRow);
			} while (true);
			return rs;
		} catch (IOException e) {
			LOG.error(e.getMessage(),e);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		} catch (Throwable th) {
			LOG.error(th.getMessage(), th);
		}finally {
			if (scanner != null)
				try {
					scanner.close();
				} catch (IOException e) {
					LOG.error(e.getMessage(),e);
				}
		}
		return null;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public ResultSet simpleQuery(
			byte[] start,
			byte[] end,
			Select selectCol, 
			String fromTable,
			Where whereCondition,
			List<OrderBy> orderbys,
			int top, 
			MetaData meta) throws IOException {
		
		RegionScanner scanner = null;
		ResultSet rs  = new ResultSet();
		try {
			Scan scan = new Scan();
			scan.setMaxVersions(1);
			scan.setStartRow(start);
			scan.setStopRow(end);
			
			// prepare scan object
			List<String> colNames = Parser.extractColumnNames(selectCol, null, 
					whereCondition,  null, null, orderbys);
			for (int i=0;i<colNames.size();++i) {
				String tmp[] = colNames.get(i).split(":");
				scan.addColumn(Bytes.toBytes(tmp[0]), Bytes.toBytes(tmp[1]));
			}
			scanner = ((RegionCoprocessorEnvironment) getEnvironment())
					.getRegion().getScanner(scan);
			
			IReader reader = new RegionScannerReader(scanner, meta, null);
			internalSimpleQuery(selectCol, fromTable, whereCondition, orderbys, top, meta, 
					rs, reader);
			
		} finally {
			if (scanner!=null)
				scanner.close();
		}
		return rs;
	}
	
	
	public void internalSimpleQuery(Select selectCol, 
			String fromTable,
			Where whereCondition,
			List<OrderBy> orderbys,
			int top, 
			MetaData meta,
			ResultSet rs, 
			IReader reader) throws IOException {
		while (true) {
			
			if (top > 0) {
				if (orderbys == null && rs.size()>=top) {
					break;
				} else if (orderbys != null && rs.size() == top + 1) {
					rs.remove(rs.size()-1);
				}
			}
			
			Row currentRow = reader.next();
			if (currentRow == null) break;
			
			//lets add calculation column
			for (int i=0;i<selectCol.getCaculation().size();++i) {
				int pos = selectCol.getCaculation().get(i);
				currentRow.doCaculation(selectCol.getColumn(pos));
			}
			// process where condition
			if (Matcher.accept(whereCondition, currentRow)) {
				rs.add(currentRow, orderbys);
			}
		} 
		
		LOG.info("CP: a scan return + " + rs.size() + " rows");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ResultSet aggregateQuery(
			byte[] start,
			byte[] end,
			AggSelect aggSelect, 
			String fromTable, 
			Where whereCondition,
			GroupBy groupbys,  
			Having having, 
			List<OrderBy> orderbys,
			MetaData meta) throws IOException {
	
		RegionScanner scanner = null;
		ResultSet rs = new ResultSet();
		try {
			Scan scan = new Scan();
			scan.setMaxVersions(1);
			scan.setStartRow(start);
			scan.setStopRow(end);
			// prepare scan object
			List<String> colNames = Parser.extractColumnNames(null, aggSelect,  whereCondition,
						groupbys, having, orderbys);
			
			for (int i=0;i<colNames.size();++i) {
				String tmp[] = colNames.get(i).split(":");
				scan.addColumn(Bytes.toBytes(tmp[0]), Bytes.toBytes(tmp[1]));
			}
			
			scanner = ((RegionCoprocessorEnvironment) getEnvironment())
					.getRegion().getScanner(scan);
			IReader reader = new RegionScannerReader(scanner, meta, null);
			internalAggQuery(aggSelect.getColumns(), fromTable, whereCondition, groupbys, having, orderbys, meta,
					rs, reader);
			
		} finally {
			if (scanner != null)
				scanner.close();
		}
		return rs;
	}
	
	public void internalAggQuery(List<Aggregate> aggColumns, 
			String fromTable, 
			Where whereCondition,
			GroupBy groupbys,  
			Having having, 
			List<OrderBy> orderbys,
			MetaData meta, 
			ResultSet rs, IReader reader) throws IOException {
		
		do {
			Row currentRow = reader.next();
		
			if (currentRow == null) break;
			//process where condition
			if (Matcher.accept(whereCondition, currentRow)) {
				//generate aggregate columns and put them into current row
				for (int i=0;i<aggColumns.size();++i) {
					Aggregate aggrgt = aggColumns.get(i);
					generateAggregate(currentRow, aggrgt);
				}
				
				if (orderbys!=null)
				for (int i=0;i<orderbys.size();++i) {
					//generate aggregate column
					OrderBy ob = orderbys.get(i);
					if (ob.getAggregate() != null) {
						generateAggregate(currentRow, ob.getAggregate());
					}
				}
				generateAggregateFromHaving(currentRow, having);
				
				if (groupbys != null) {
					//splice group by columns
					byte[] buff = new byte[0];  
					List<String> grpColumns = groupbys.getColumns();
					for (int i=0;i<grpColumns.size();++i) {
						buff = Bytes.add(buff, currentRow.getColumn(grpColumns.get(i)).getValue());
					}
					currentRow.addColumn(new Column(Column.SPLICE_KEY_COL_NAME, 
							buff, Column.DataType.AGGKEY));
				}
				rs.accumulate(currentRow);
			}
		} while (true);
	}
	
	
	private void generateAggregateFromHaving(Row currentRow, Having having) {
		if (having==null) return;
		Aggregate aggrgt = having.getAggregate();
		if (aggrgt!=null)
			generateAggregate(currentRow, aggrgt);
		else {
			if (having.getLeft() != null) {
				generateAggregateFromHaving(currentRow, having.getLeft());
			} else if (having.getRight() != null) {
				generateAggregateFromHaving(currentRow, having.getRight());
			}
		}
	}
	
	private void generateAggregate(Row currentRow, Aggregate aggrgt) {
		if (currentRow == null || aggrgt == null) return ;
		Column original = currentRow.getColumn(aggrgt.getColName());
		if (original == null) {
			return;
		}
		//generate aggregate columns
		String fakeName = aggrgt.getFakeName();
		if (!currentRow.hasColumn(fakeName)) {
			if (aggrgt.getType() == Expression.AGG_AVG ) {
				//if this is an average column, we need to add a count column 
				//and a sum column because avg=sum/count, 
				if (!currentRow.hasColumn(Aggregate.PREFIX_SUM+aggrgt.getColName())) {
					Column newAggCol=new Column(Aggregate.PREFIX_SUM+aggrgt.getColName() , 
							original.getValue(), original.getType());
					newAggCol.setAggType(Aggregate.AGG_SUM);
					currentRow.addColumn(newAggCol);
				}
				if (!currentRow.hasColumn(Aggregate.PREFIX_COUNT)) {
					Column newAggCol = new Column(Aggregate.PREFIX_COUNT , 
							original.getValue(), original.getType());
					newAggCol.setAggType(Aggregate.AGG_COUNT);
					currentRow.addColumn(newAggCol);
				}
			} 
			Column newAggCol = new Column(fakeName, 
					original.getValue(), original.getType());
			newAggCol.setAggType(aggrgt.getType());
			currentRow.addColumn(newAggCol);
		}
	}
}