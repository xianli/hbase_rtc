package com.xl.hbase.rtc;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import com.xl.hbase.rtc.expression.Expression;
import com.xl.hbase.rtc.model.Column;

import static org.junit.Assert.*;

public class ColumnTest {
	
	Column ac ;
	Column bc ;
	Column cc ;
	
	@Before public void init () {
		ac = new Column("age",Bytes.toBytes(20), "int");
		bc = new Column("age", Bytes.toBytes(30l), "long");
		cc = new Column("age", Bytes.toBytes(100.2d) ,"double");
	}
	
	@Test public void testSum() {
		ac.setAggType(Expression.AGG_SUM);
		bc.setAggType(Expression.AGG_SUM);
		cc.setAggType(Expression.AGG_SUM);
		ac.accumulate(bc);
		ac.accumulate(cc);
		assertEquals(150l, Bytes.toLong(ac.getValue()));
	}
	
	@Test public void testMax() {
		ac.setAggType(Expression.AGG_MAX);
		bc.setAggType(Expression.AGG_MAX);
		cc.setAggType(Expression.AGG_MAX);
		ac.accumulate(bc);
		ac.accumulate(cc);
		assertEquals(100, Bytes.toInt(ac.getValue()));
	}
	
	@Test public void testMin() {
		ac.setAggType(Expression.AGG_MIN);
		bc.setAggType(Expression.AGG_MIN);
		cc.setAggType(Expression.AGG_MIN);
		ac.accumulate(bc);
		ac.accumulate(cc);
		assertEquals(20, Bytes.toInt(ac.getValue()));
	}
	
	@Test public void testDivide() {
		System.out.println(Bytes.toDouble(ac.divide(bc)));
		System.out.println(Bytes.toDouble(ac.divide(cc)));
	}
	
	@Test public void testMultiply() {
		System.out.println(Bytes.toDouble(ac.multiply(bc)));
		System.out.println(Bytes.toDouble(ac.multiply(cc)));
	}
	
	@Test public void testPlus() {
		System.out.println(Bytes.toDouble(ac.plus(bc)));
		System.out.println(Bytes.toDouble(ac.plus(cc)));
	}
	
	@Test public void testMinus() {
		System.out.println(Bytes.toDouble(ac.minus(bc)));
		System.out.println(Bytes.toDouble(ac.minus(cc)));
	}
}
