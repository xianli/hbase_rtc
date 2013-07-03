package com.xl.hbase.rtc.util;

import org.apache.hadoop.hbase.util.Bytes;

import com.xl.hbase.rtc.expression.Expression;
import com.xl.hbase.rtc.expression.Having;
import com.xl.hbase.rtc.expression.Where;
import com.xl.hbase.rtc.model.Row;

public class Matcher {
	
	private  static int getCompareResult(Having condition, Row row) {
		byte[] expectedValue = condition.getExpectedValue();
		byte[] actualValue = row.getColumn(condition.getAggregate().getFakeName()).getValue();
		return Bytes.compareTo(actualValue, expectedValue);
	}
	
	private static int getCompareResult(Where condition, Row row) {
		byte[] expectedValue = condition.getExpectedValue();
		byte[] actualValue = row.getColumn(condition.getColName()).getValue();
		return Bytes.compareTo(actualValue, expectedValue);
	}
	
	public static boolean accept(Having condition, Row row) {
		
		if (condition == null) return true;
		int operator = condition.getOperator();
		int result;
		boolean rtn = true;
		switch (operator) {
			case Expression.LOGIC_LESS:
				result = getCompareResult(condition, row);
				rtn = (result<0) ? true : false;
				break;
			case Expression.LOGIC_GREATER: 
				result = getCompareResult(condition, row);
				rtn =(result>0) ? true : false ; 
				break;
			case Expression.LOGIC_LESS_OR_EQUAL:
				result = getCompareResult(condition, row);
				rtn = (result<=0) ? true : false;
				break;
			case Expression.LOGIC_GREATER_OR_EQUAL:
				result = getCompareResult(condition, row);
				rtn = (result>=0) ? true : false;
				break;
			case Expression.LOGIC_NOT_EQUAL:
				result = getCompareResult(condition, row);
				rtn = (result!=0) ? true : false;
				break;
			case Expression.LOGIC_EQUAL:
				result = getCompareResult(condition, row);
				rtn = (result==0) ? true : false;
				break;
			case Expression.LOGIC_AND :
				rtn = accept(condition.getLeft(), row) && accept(condition.getRight(), row) ;
				break;
			case Expression.LOGIC_OR :
				rtn = accept(condition.getLeft(), row) || accept(condition.getRight(), row);
				break;
		}
		return rtn;
	}
	
	public static boolean accept(Where condition, Row row) {
		if (condition == null) return true;
		int operator = condition.getOperator();
		int result;
		boolean rtn = true;
		switch (operator) {
			case Expression.LOGIC_LESS:
				result = getCompareResult(condition, row);
				rtn = (result<0) ? true : false;
				break;
			case Expression.LOGIC_GREATER: 
				result = getCompareResult(condition, row);
				rtn =(result>0) ? true : false ; 
				break;
			case Expression.LOGIC_LESS_OR_EQUAL:
				result = getCompareResult(condition, row);
				rtn = (result<=0) ? true : false;
				break;
			case Expression.LOGIC_GREATER_OR_EQUAL:
				result = getCompareResult(condition, row);
				rtn = (result>=0) ? true : false;
				break;
			case Expression.LOGIC_NOT_EQUAL:
				result = getCompareResult(condition, row);
				rtn = (result!=0) ? true : false;
				break;
			case Expression.LOGIC_EQUAL:
				result = getCompareResult(condition, row);
				rtn = (result==0) ? true : false;
				break;
			case Expression.LOGIC_AND :
				rtn = accept(condition.getLeft(), row) && accept(condition.getRight(), row) ;
				break;
			case Expression.LOGIC_OR :
				rtn = accept(condition.getLeft(), row) || accept(condition.getRight(), row);
				break;
		}
		return rtn;
	}
}
