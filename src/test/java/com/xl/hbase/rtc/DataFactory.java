package com.xl.hbase.rtc;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class DataFactory {

	/**
	 * 
	 * @param start e.g., 20120101
	 * @param end e.g., 20121231
	 * @return
	 */
	public static List<Long> createDate(long start, long end) {
		List<Long> dates = new ArrayList<Long>();
		for (long i=start;i<=end;i++) {
			if (isDate(i))
				dates.add(i);
		}
		return dates;
	}
	
	private static boolean isDate(long i) {
		
		long year = i/10000;
		long month = (i-year*10000)/100;
		long day = i-(year*100+month)*100;
		
		if (month>12 || month < 1) return false;
		if (day>31 || day < 1) return false;
		if (month==2) {
			if (year % 4 == 0 && year % 100 != 0) {
				//leap year
				if (day > 29) return false;
			} else if (day > 28)
				return false;
		}
		return true;
	}

	public static List<Long> createSequence(long start, long end) {
		List<Long> ids = new ArrayList<Long>();
		for (long i=start;i<=end;i++)
			ids.add(i);
		return ids;
	}
	
	public static void printList(List<?> list) {
		Iterator<?>it = list.iterator();
		while (it.hasNext()) {
			System.out.print(it.next()+",");
		}
	}
	
	
	public static void main(String[] args) {
		printList(createSequence(10001, 10020));
		printList(createDate(20120101, 20120301));
	}
}
