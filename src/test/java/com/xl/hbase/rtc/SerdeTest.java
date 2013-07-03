package com.xl.hbase.rtc;

import static com.xl.hbase.rtc.expression.ExpressionFactory.aggcal;
import static com.xl.hbase.rtc.expression.ExpressionFactory.and;
import static com.xl.hbase.rtc.expression.ExpressionFactory.asc;
import static com.xl.hbase.rtc.expression.ExpressionFactory.avg;
import static com.xl.hbase.rtc.expression.ExpressionFactory.cal;
import static com.xl.hbase.rtc.expression.ExpressionFactory.eq;
import static com.xl.hbase.rtc.expression.ExpressionFactory.groupby;
import static com.xl.hbase.rtc.expression.ExpressionFactory.gt;
import static com.xl.hbase.rtc.expression.ExpressionFactory.having;
import static com.xl.hbase.rtc.expression.ExpressionFactory.lt;
import static com.xl.hbase.rtc.expression.ExpressionFactory.or;
import static com.xl.hbase.rtc.expression.ExpressionFactory.select;
import static com.xl.hbase.rtc.expression.ExpressionFactory.sum;

import static org.junit.Assert.assertEquals;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import com.jd.bi.odp.data.hbase.reader.ColumnMeta;
import com.jd.bi.odp.data.hbase.reader.MetaConfig;
import com.xl.hbase.rtc.expression.AggSelect;
import com.xl.hbase.rtc.expression.Expression;
import com.xl.hbase.rtc.expression.GroupBy;
import com.xl.hbase.rtc.expression.Having;
import com.xl.hbase.rtc.expression.OrderBy;
import com.xl.hbase.rtc.expression.Select;
import com.xl.hbase.rtc.expression.Where;
import com.xl.hbase.rtc.model.Column;
import com.xl.hbase.rtc.model.ResultSet;
import com.xl.hbase.rtc.model.Row;
/**
 * @author jiangxl
 */
public class SerdeTest {

	DataOutput out ;
	DataInput in ;
	
	Column serCol;
	Column serAggCol;
	Row serRow;
	
	ResultSet serRS;
	
	@Before public void init() {
		try {
			out = new DataOutputStream(new FileOutputStream("test"));
			in = new DataInputStream(new FileInputStream("test"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		serCol = new Column("name", Bytes.toBytes("this is a test"), "string");
		serAggCol = new Column("sum_name", Bytes.toBytes(100), "int");
		serAggCol.setAggType(Expression.AGG_SUM);
		
		serRow = new Row();
		serRow.addColumn(serCol);
		serRow.addColumn(serAggCol);
		
		serRS = new ResultSet();
		serRS.add(serRow);
		serRS.add(serRow);
	}
	
	@Test public void testColumnMeta() throws FileNotFoundException, IOException {
		List<ColumnMeta> metas = MetaConfig.getColumnsMeta("HB_TEAM_SALE", "getTeamSale");
		for (int i=0;i<metas.size();i++) {
			metas.get(i).write(out);
		}
		for (int i=0;i<metas.size();i++) {
			ColumnMeta cm = new ColumnMeta();
			cm.readFields(in);
			System.out.println(cm.getFullColName());
		}
	}
	
	@Test public void testRSSer() {
		ResultSet rs = new ResultSet();
		try {
			serRS.write(out);
			rs.readFields(in);
			assertEquals(rs.size(), 2);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	@Test public void testRow() {
		Row deserRow = new Row();
		try {
			serRow.write(out);
			deserRow.readFields(in);
			assertEquals(deserRow.getColumn("name").getType(), "string");
			assertEquals(Bytes.compareTo(deserRow.getColumn("name").getValue(), serCol.getValue()), 0);
			assertEquals(deserRow.getColumn("sum_name").getAggType(), Expression.AGG_SUM);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test public void testColumn() {
		Column deser=new Column();
		try {
			serCol.write(out);
			deser.readFields(in);
			assertEquals(serCol.getName(), deser.getName());
			assertEquals(serCol.getType(), deser.getType());
			assertEquals(serCol.getValue().length, deser.getValue().length);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test public void testSelect() {
		Select selectCol = select("d:UPV","d:CancelAmt",
				"d:PurchaseAmt","_row_0",
				"_row_1","_row_2", "d:PurchaseOrderNum", cal("(d:VerySatisNum+d:SatisNum)/(d:VerySatisNum+d:SatisNum+d:OrdiNum+d:NotSatisNum+d:VeryNotSatisNum+d:NotCommNum)"));
		try {
			selectCol.write(out);
			Select another = new Select();
			another.readFields(in);
			assertEquals(another.getColumn(0), "d:UPV");
			assertEquals(another.getColumn(1), "d:CancelAmt");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test public void testWhere() {
		Where c1 =  eq("_row_3", 20120618);
		Where c4 = or(gt("d:UPV", 100), 
				and(lt("d:CancelAmt", 100.0d), 
				gt("d:PurchaseAmt", 19812.0d)));
		
		try {
			c1.write(out);
			c4.write(out);
			
			Where another = new Where();
			another.readFields(in);
			assertEquals(another.getOperator(), Expression.LOGIC_EQUAL);
			another = new Where();
			another.readFields(in);
			assertEquals(another.getLeft().getColName(), "d:UPV");
			assertEquals(another.getRight().getOperator(), Expression.LOGIC_AND);
			assertEquals(Bytes.toDouble(another.getRight().getLeft().getExpectedValue()), 100.0d);
			assertEquals(another.getOperator(), Expression.LOGIC_OR);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test public void testOrderBy() {
		OrderBy ob = asc("d:UPV");
		OrderBy ob2 = asc(sum("d:Money"));
		try {
			ob.write(out);
			ob2.write(out);
			OrderBy another  = new OrderBy();
			another.readFields(in);
			assertEquals(another.getColName(), "d:UPV");
			another = new OrderBy();
			another.readFields(in);
			assertEquals(another.getAggregate().getColName(), "d:Money");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test public void testGroupby() {
		
		GroupBy groupbys = groupby("_row_2");
		try {
			groupbys.write(out);
			GroupBy another = new GroupBy();
			another.readFields(in);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	@Test public void testAggSelect() {
		AggSelect aggSelect=select(sum("d:UPV"),sum("d:CancelAmt"), avg("d:PurchaseAmt"),
				aggcal("sum(d:CancelAmt)/avg(d:PurchaseAmt)" )); 
		
		try {
			aggSelect.write(out);
			AggSelect another = new AggSelect();
			another.readFields(in);
			assertEquals(another.getColumn(0).getColName(), "d:UPV");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	@Test public void testHavings() {
		Having havings = having( lt(sum("d:CancelAmt"), 6104.0));
		
		try {
			havings.write(out);
			Having another = new Having();
			another.readFields(in);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
