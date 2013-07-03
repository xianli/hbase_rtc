package com.xl.hbase.rtc.expression;

import java.util.ArrayList;
import java.util.List;


import com.jd.bi.odp.data.hbase.inf.SortedKVMap;
import com.xl.hbase.rtc.util.SuffixExp;


/**
 * 
 * <b>some examples: </b><br> 
 * 
 *  select(as("col_a", "name"), "col_b", divide("col_a","col_b"))  <br> 
 *  aslist(sum("a"), avg("b"))  <br>
 *  where(and(lte("col_c", "haha"), gt("col_d", 123)))  <br>
 *  groupby("col_e")  <br>
 *  having(sum("a")>10) <br>
 *  orderby(asc(avg("col_f")))  <br>
 *  
 * @author jiangxl 
 */
public class ExpressionFactory {
	
	//less than or equal
	public static Where lte(String colName, byte value) {
		return new Where(colName, value, Expression.LOGIC_LESS_OR_EQUAL);
	}
	
	//great than or equal
	public static Where gte(String colName, byte value) {
		return new Where(colName, value, Expression.LOGIC_GREATER_OR_EQUAL);
	}
	
	public static Where lt(String colName, byte value) {
		return new Where(colName, value, Expression.LOGIC_LESS);
	}
	
	public static Where gt(String colName, byte value) {
		return new Where(colName, value, Expression.LOGIC_GREATER);
	}
	
	//not equal
	public static Where neq(String colName, byte value) {
		return new Where(colName, value, Expression.LOGIC_NOT_EQUAL);
	}
	
	public static Where eq(String colName, byte value) {
		return new Where(colName, value, Expression.LOGIC_EQUAL);
	}
	

	public static Where lte(String colName, long value) {
		return new Where(colName, value, Expression.LOGIC_LESS_OR_EQUAL);
	}
	
	public static Where gte(String colName, long value) {
		return new Where(colName, value, Expression.LOGIC_GREATER_OR_EQUAL);
	}
	
	public static Where lt(String colName, long value) {
		return new Where(colName, value, Expression.LOGIC_LESS);
	}
	
	public static Where gt(String colName, long value) {
		return new Where(colName, value, Expression.LOGIC_GREATER);
	}
	
	public static Where neq(String colName, long value) {
		return new Where(colName, value, Expression.LOGIC_NOT_EQUAL);
	}
	
	public static Where eq(String colName, long value) {
		return new Where(colName, value, Expression.LOGIC_EQUAL);
	}
	
	public static Where lte(String colName, double value) {
		return new Where(colName, value, Expression.LOGIC_LESS_OR_EQUAL);
	}
	
	public static Where gte(String colName, double value) {
		return new Where(colName, value, Expression.LOGIC_GREATER_OR_EQUAL);
	}
	
	public static Where lt(String colName, double value) {
		return new Where(colName, value, Expression.LOGIC_LESS);
	}
	
	public static Where gt(String colName, double value) {
		return new Where(colName, value, Expression.LOGIC_GREATER);
	}
	
	public static Where neq(String colName, double value) {
		return new Where(colName, value, Expression.LOGIC_NOT_EQUAL);
	}
	
	public static Where eq(String colName, double value) {
		return new Where(colName, value, Expression.LOGIC_EQUAL);
	}
	
	public static Where lte(String colName, int value) {
		return new Where(colName, value, Expression.LOGIC_LESS_OR_EQUAL);
	}
	
	public static Where gte(String colName, int value) {
		return new Where(colName, value, Expression.LOGIC_GREATER_OR_EQUAL);
	}
	
	public static Where lt(String colName, int value) {
		return new Where(colName, value, Expression.LOGIC_LESS);
	}
	
	public static Where gt(String colName, int value) {
		return new Where(colName, value, Expression.LOGIC_GREATER);
	}
	
	public static Where neq(String colName, int value) {
		return new Where(colName, value, Expression.LOGIC_NOT_EQUAL);
	}
	
	public static Where eq(String colName, int value) {
		return new Where(colName, value, Expression.LOGIC_EQUAL);
	}
	
	public static Where neq(String colName, String value) {
		return new Where(colName, value, Expression.LOGIC_NOT_EQUAL);
	}
	
	public static Where eq(String colName, String value) {
		return new Where(colName, value, Expression.LOGIC_EQUAL);
	}
	
	// and(and(lt(cola, 123), gt(colb, 234)), eq(colc, 123));
	public static Where and(Where left, Where right) {
		return new Where(left, right, Expression.LOGIC_AND);
	}
	
	public static Where or(Where left, Where right) {
		return new Where(left, right, Expression.LOGIC_OR);
	}
	
	/*aggregate expression*/
	public static Having lte(Aggregate colName, long value) {
		return new Having(colName, value, Expression.LOGIC_LESS_OR_EQUAL);
	}
	
	public static Having gte(Aggregate colName, long value) {
		return new Having(colName, value, Expression.LOGIC_GREATER_OR_EQUAL);
	}
	
	public static Having lt(Aggregate colName, long value) {
		return new Having(colName, value, Expression.LOGIC_LESS);
	}
	
	public static Having gt(Aggregate colName, long value) {
		return new Having(colName, value, Expression.LOGIC_GREATER);
	}
	
	public static Having neq(Aggregate colName, long value) {
		return new Having(colName, value, Expression.LOGIC_NOT_EQUAL);
	}
	
	public static Having eq(Aggregate colName, long value) {
		return new Having(colName, value, Expression.LOGIC_EQUAL);
	}
	
	public static Having lte(Aggregate colName, double value) {
		return new Having(colName, value, Expression.LOGIC_LESS_OR_EQUAL);
	}
	
	public static Having gte(Aggregate colName, double value) {
		return new Having(colName, value, Expression.LOGIC_GREATER_OR_EQUAL);
	}
	
	public static Having lt(Aggregate colName, double value) {
		return new Having(colName, value, Expression.LOGIC_LESS);
	}
	
	public static Having gt(Aggregate colName, double value) {
		return new Having(colName, value, Expression.LOGIC_GREATER);
	}
	
	public static Having neq(Aggregate colName, double value) {
		return new Having(colName, value, Expression.LOGIC_NOT_EQUAL);
	}
	
	public static Having eq(Aggregate colName, double value) {
		return new Having(colName, value, Expression.LOGIC_EQUAL);
	}
	
	public static Having lte(Aggregate colName, int value) {
		return new Having(colName, value, Expression.LOGIC_LESS_OR_EQUAL);
	}
	
	public static Having gte(Aggregate colName, int value) {
		return new Having(colName, value, Expression.LOGIC_GREATER_OR_EQUAL);
	}
	
	public static Having lt(Aggregate colName, int value) {
		return new Having(colName, value, Expression.LOGIC_LESS);
	}
	
	public static Having gt(Aggregate colName, int value) {
		return new Having(colName, value, Expression.LOGIC_GREATER);
	}
	
	public static Having neq(Aggregate colName, int value) {
		return new Having(colName, value, Expression.LOGIC_NOT_EQUAL);
	}
	
	public static Having eq(Aggregate colName, int value) {
		return new Having(colName, value, Expression.LOGIC_EQUAL);
	}
	
	public static Having and(Having left, Having right) {
		return new Having(left, right, Expression.LOGIC_AND);
	}
	
	public static Having or(Having left, Having right) {
		return new Having(left, right, Expression.LOGIC_OR);
	}
	
	public static Aggregate sum(String colName) {
		return new Aggregate(colName, Expression.AGG_SUM);
	}

	public static Aggregate avg(String colName) {
		return new Aggregate(colName, Expression.AGG_AVG);
	}
	
	public static Aggregate cnt(String colName) {
		return new Aggregate(colName, Expression.AGG_COUNT);
	}
	
	public static Aggregate max(String colName) {
		return new Aggregate(colName, Expression.AGG_MAX);
	}
	
	public static Aggregate min(String colName) {
		return new Aggregate(colName, Expression.AGG_MIN);
	}

	public static Aggregate out(String colName) {
		return new Aggregate(colName, Expression.AGG_OUT);
	}
	
	/**
	 * 
	 * @param calculation  sum(a) + sum(b) / sum(c) * 100 + 0.2
	 * @return
	 */
	public static Aggregate aggcal(String calculation) {
		return new Aggregate(SuffixExp.getSuffixExp(calculation), 
				Expression.AGG_CAL);
	}
	
	/**
	 * 
	 * 
	 * @param calculation   (column_a + column_b) / column_c * 100 + 0.2 ;
	 * @return
	 */
	public static String cal(String calculation) {
		//change it to suffix expression;
		return SuffixExp.getSuffixExp(calculation);
	}
 
	public static OrderBy desc(Aggregate agg) { 
		return new OrderBy(agg, Expression.ORD_DESC);
	}
	
	public static OrderBy desc(String column) {
		return new OrderBy(column, Expression.ORD_DESC);
	}
	
	public static OrderBy asc(Aggregate agg) {
		return new OrderBy(agg, Expression.ORD_ASC);
	}
	
	public static OrderBy asc(String column) {
		return new OrderBy(column, Expression.ORD_ASC);
	}
	
	public static Lookup lookup(String colName, SortedKVMap dimension, String alias) {
		Lookup lk = new Lookup();
		lk.setColName(colName);
		lk.setDim(dimension);
		lk.setAlias(alias);
		return lk;
	}
	
	public static Select select(String ... columns) {
		Select select = new Select();
		
		for (int i=0;i<columns.length;++i) {
			select.addColumn(columns[i]);
		}
		return select;
	}
	
	public static AggSelect select(Aggregate ... columns) {
		AggSelect select = new AggSelect();
		
		for (int i=0;i<columns.length;++i) {
			select.addColumn(columns[i]);
		}
		return select;
	}
	
	public static Where where(Where condition) {
		return condition;
	}
	
	public static GroupBy groupby(String...groupbys) {
		GroupBy gp = new GroupBy();
		for (int i=0;i<groupbys.length;++i) {
			gp.addColumn(groupbys[i]);
		}
		return gp;
	}
	
	public static Having having(Having condition) {
		return condition;
	}
	
	public static String as(String colName, String alias) {
		return colName+Expression.DELIM+alias;
	}
	
	public static Aggregate as(Aggregate agg, String alias) {
		agg.setAlias(alias);
		return agg;
	}

	public static <T> List<T> aslist(T ...elements) {
		List<T> list = new ArrayList<T>(); 
		for (int i=0;i<elements.length;++i) {
			list.add(elements[i]);
		}
		return list;
	}
	
	public static void main(String[] args) {
		System.out.println(as("hello","world"));
	}
}
