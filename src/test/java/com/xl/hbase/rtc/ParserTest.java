package com.xl.hbase.rtc;

import java.util.List;

import org.junit.Test;

import com.xl.hbase.rtc.expression.AggSelect;
import com.xl.hbase.rtc.expression.Aggregate;
import com.xl.hbase.rtc.expression.GroupBy;
import com.xl.hbase.rtc.expression.Having;
import com.xl.hbase.rtc.expression.OrderBy;
import com.xl.hbase.rtc.expression.Select;
import com.xl.hbase.rtc.expression.Where;
import com.xl.hbase.rtc.util.Parser;

import static com.xl.hbase.rtc.expression.ExpressionFactory.*;

public class ParserTest {
	
	@Test public void testSimple() {
		Select selectCol = select("d:UPV","d:CancelAmt",
				"d:PurchaseAmt","_row_0",
				"_row_1","_row_2", "d:OrderNum", "d:TeamStartTime", as("d:PurchaseOrderNum", "123"), 
				as(cal("d:OrderNum/d:OrderAmt"), "testdiv"), cal("d:cal_1+d:cal_2/d:cal_3"));
		Where whereCondition = or(gt("d:UPV", 100), and(lt("d:CancelAmt", 100.0d), gt("d:PurchaseAmt", 19812.0d)));
		List<OrderBy> orderbys = aslist(asc("d:UPV"), asc("d:CancelAmt"));
		
		List<String> names = Parser.extractColumnNames(selectCol, null, whereCondition,  null, null, orderbys);
		for (int i=0;i<names.size();i++) {
			System.out.println(names.get(i));
		}
	}
	
	@Test public void testAgg() {
//		Select selectCol = select("d:UPV","d:CancelAmt",
//				"d:PurchaseAmt","_row_0",
//				"_row_1","_row_2", "d:TeamStartTime", "d:PurchaseOrderNum");
		Where whereCondition = or(gt("d:UPV", 100), and(lt("d:CancelAmt", 100.0d), gt("d:PurchaseAmt", 19812.0d)));
		AggSelect aggs = select(sum("d:sum_1"), avg("d:avg_2"), max("d:min_3"), min("d:max_4"), aggcal("sum(d:cal_1)+sum(d:cal_2)/sum(d:cal_3)"));
		GroupBy groupby = groupby("d:sum_1", "d:avg_2");
		Having havings = and(or (and(eq(sum("d:sum_1"),123), eq(avg("d:avg_2"),123)), gt(max("d:test"),23432)), 
				gt( aggcal("sum(d:cal_1)+sum(d:cal_2)/sum(d:cal_3)"), 123));
		List<OrderBy> orderbys = aslist(asc("d:UPV"), asc("d:CancelAmt"));
		List<String> names = Parser.extractColumnNames(null, aggs, whereCondition,  groupby, havings, orderbys);
//		for (int i=0;i<names.size();i++) {
//			System.out.println(names.get(i));
//		}
		
		for (int i=0;i<aggs.getColumns().size();i++) {
			System.out.println(aggs.getColumns().get(i).getFakeName());
		}
	}
}
