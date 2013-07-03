package com.xl.hbase.rtc;

import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.xl.hbase.rtc.model.Column;
import com.xl.hbase.rtc.model.Row;
import com.xl.hbase.rtc.util.SuffixExp;

public class SuffixExpTest {

	
	@Test public void testGetTokens() {
		List<String> tt = SuffixExp.getTokens("sum(a) 	+ avg(b) / sum(c)");
		for (int i=0;i<tt.size();++i) System.out.print(tt.get(i)+",");
		System.out.println();
		tt = SuffixExp.getTokens("(suma+avgb)/(sumc-asdf)");
		for (int i=0;i<tt.size();++i) System.out.print(tt.get(i)+",");
	}
	
	@Test public void testGetSuffix() {
		System.out.println(SuffixExp.getSuffixExp("sum(a)+avg(b)/sum(d)+sum(c)*100"));
		//System.out.println(Calculator.getSuffixExp("sum(a)+avg(b)/sum(c)"));
	}
	
	@Test public void testCalculation() {
		Column a = new Column("a", Bytes.toBytes(100), "int");
		Column b = new Column("b", Bytes.toBytes(200), "int");
		Column c = new Column("c", Bytes.toBytes(300), "int");
		
		Row row = new Row();
		row.addColumn(a);
		row.addColumn(b);
		row.addColumn(c);
		row.doCaculation(SuffixExp.getSuffixExp("a+b/c"));
		row.print(System.out);
		System.out.println();
		row.doCaculation(SuffixExp.getSuffixExp("(a+b)/c"));
		row.print(System.out);
		System.out.println();
	}
	
	@Test public void test() {
		Assert.assertEquals(true, ("1000".matches("[1-9]\\d*.?\\d*")));
		Assert.assertEquals(true, ("1000.123".matches("[1-9]\\d*.?\\d*")));
		Assert.assertEquals(false, ("0123".matches("[1-9]\\d*.?\\d*")));
		Assert.assertEquals(false, ("1000.123.123".matches("[1-9]\\d*.?\\d*")));
	}
}
