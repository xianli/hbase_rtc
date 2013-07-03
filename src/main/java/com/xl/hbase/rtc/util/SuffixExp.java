package com.xl.hbase.rtc.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.apache.log4j.Logger;

import com.xl.hbase.rtc.expression.Expression;


/**
 * <ol>
 * <li>replace aggregate columns with their fake name if any </li>
 * <li>convert the infix expression to a suffix expression </li>
 * </ol>
 * 
 <pre>
 infix                  suffix                        prefix
---------------------------------------------------------------------------
suma+sumb-sumc    | suma, sumb, +, sumc, -    |  -, sumc, +, suma, sumb 
---------------------------------------------------------------------------
suma+sumb/sumc    |  suma sumb sumc / +   	   | +,  /, suma, sumb, sumc 
---------------------------------------------------------------------------
 </pre>
 *
 */
public class SuffixExp {

	protected static Logger LOG = Logger.getLogger(SuffixExp.class);
	
	public static String getSuffixExp(String calExp) {
		List<String> tokens = getTokens(calExp);
		Stack<String> operator = new Stack<String>();
		
		StringBuilder rtnExp = new StringBuilder();
		for (int i=0;i<tokens.size();++i) {
			String token = tokens.get(i);
			
			if (token.equals("+") || token.equals("-") 
					|| token.equals("*") || token.equals("/") ) {
				while (!operator.empty() && 
						oprPriority(token, operator.peek()) < 0) {
					//pop until token > operator.peek()
					rtnExp.append(operator.pop()+Expression.TOKEN_SP);
				}
				 operator.push(token); 
			} else if (token.equals("(")) {
				operator.push(token);
			} else if (token.equals(")")) {
				//pop until meet "("
				while (!operator.empty()) {
					String top = operator.pop();
					if (top.equals("(")) break;
					rtnExp.append(top+Expression.TOKEN_SP);
				}
			} else {
				//it is operand
				rtnExp.append(token+Expression.TOKEN_SP);
			}
		}
		
		while (!operator.empty()) {
			rtnExp.append(operator.pop()+Expression.TOKEN_SP);
		}
		
		//remove the last SP
		if (rtnExp.length()>1) rtnExp.deleteCharAt(rtnExp.length()-1);
		return rtnExp.toString();
	}
	
	/**
	 * compare the priority of the two operators (token and peek in the stack)
	 * <br>
	 *  
	 * @param token
	 * @param peek
	 * @return  1: priority(token) > priority(peek), -1: priority(token) < priority(peek),
	 * =0:  priority(token) = priority(peek)
 	 */
	private static int oprPriority(String token, String peek) {
		int left, right;
		if (token.equals("+") || token.equals("-")) left=1;
		else left=2;
		if (peek.equals("*") || peek.equals("/")) right = 2;
		else right = 1;
//		LOG.debug(token+" minus "+peek+" equal "+(left-right));
		return left-right;
	}


	public static List<String> getTokens(String calExp) {
		String[] tokens = getAllTokens(calExp);
		List<String> finals = new ArrayList<String>();
		for (int i=0;i<tokens.length;i++) {
			String token = tokens[i];
			if (token.equals("sum")||token.equals("avg")||
					token.equals("max")||token.equals("min")||
					token.equals("cnt")) {
				//eat the next three tokens; they should be "("+column name+")"
				if (tokens[i+1].equals("(") && tokens[i+3].equals(")")) {
					token = "_#"+token+"#"+tokens[i+2];
					i+=3;
				} else {
					throw new RuntimeException("invalid expression: " + calExp);
				}
				finals.add(token);
			} else if (!token.equals("")) {
				finals.add(token);
			} 
		}
		return finals;
	}
	
	private static String[] getAllTokens(String calExp) {
		StringBuilder builder = new StringBuilder();
		int parentheses = 0;
		for (int i=0;i<calExp.length();i++) {
			char c = calExp.charAt(i);
			if (! (c==' ' || c=='\t')) {
				switch (c) {
				case '(' : parentheses++; builder.append(Expression.TOKEN_SP)
							.append(c).append(Expression.TOKEN_SP); break;
				case ')' : parentheses--; builder.append(Expression.TOKEN_SP)
							.append(c).append(Expression.TOKEN_SP); break;
				case '+' : 
				case '-' : 
				case '*' : 
				case '/' : builder.append(Expression.TOKEN_SP).append(c)
							.append(Expression.TOKEN_SP); break;
				default: builder.append(c);
				}
			} 
		}
		if (parentheses != 0) {
			throw new RuntimeException("parentheses not match in " + calExp);
		}
		return builder.toString().split("\\"+Expression.TOKEN_SP);
	} 
	
}
