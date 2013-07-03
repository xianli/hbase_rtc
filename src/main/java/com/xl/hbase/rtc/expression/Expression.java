package com.xl.hbase.rtc.expression;

import org.apache.hadoop.io.Writable;

/**
 *  _row_* is keyword in expression. e.g., _row_0 is the first part of row key.
 * _row_1 is the second part... 
 * 
 * select a.id, b.name
 * from a, b
 * where a.id=b.id
 * where
 * 
 * @author jiangxl
 */
public interface Expression extends Writable {
	
	public int 
	LOGIC_AND = 100, 
	LOGIC_OR=101, 
	LOGIC_EQUAL=102, 
	LOGIC_NOT_EQUAL=103, 
	LOGIC_LESS=104, 
	LOGIC_LESS_OR_EQUAL=105, 
	LOGIC_GREATER=106, 
	LOGIC_GREATER_OR_EQUAL=107,
	AGG_SUM=201, AGG_AVG=202, 
	AGG_COUNT=203, 
	AGG_MAX=204, 
	AGG_MIN=205,
	AGG_DIV=206,
	AGG_CAL=207,
	AGG_OUT = 208,
	
	ORD_ASC=301,
	ORD_DESC=302,
	TOP=401,
	DIVIDE=501;
	
	public String DIV_OPERATOR="/";
	public char CAL= 0x2;
	public char DELIM = 0x1;
	
	public char TOKEN_SP=',';
	
	public String PREFIX_LK="_lk";
	public String DIGIT="[1-9]\\d*.?\\d*";
}
 