package com.xl.hbase.rtc.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.util.Bytes;

public class Util {

	/**
	 * @param bytes
	 * @param type  e.g., "int,int"
	 * @return
	 */
	public static String bytes2String(byte[] bytes, String type) {
		assert bytes != null && type != null;
		String[] types = type.split("\\,");
		StringBuilder bd = new StringBuilder();
		for (int j=0, start=0;j<types.length;j++) {
			if (types[j].equals("int")) {
				bd.append(Bytes.toInt(Arrays.copyOfRange(bytes, start, start+4)));
				start+=4;
			} else if (types[j].equals("long")) {
				bd.append(Bytes.toLong(Arrays.copyOfRange(bytes, start, start+8)));
				start+=8;
			} else if (types[j].equals("byte")) {
				bd.append(bytes[start]);
				start+=1;
			} else if (types[j].equals("string")) {
				bd.append(Bytes.toString(Arrays.copyOfRange(bytes, start, bytes.length)));
			}
			bd.append(",");
		}
		return bd.toString();
	}
	
	public static List<String> readStrings(byte[] val) {
		int start = 0, len=0;
		List<String> strings = new ArrayList<String>();
		while (start<val.length) {
			len =  Bytes.toInt(Arrays.copyOfRange(val, start, 4));
			strings.add(Bytes.toString(val, start+4, len));
			start+=len+4;
		}
		return strings;
	}
	
	public static String readString(byte[] val, int idx) {
		return readStrings(val).get(idx);
	}
	
	public static byte[] writeStrings(List<String> strs) {
		byte[] sum=new byte[0];
		for (int i=0;i<strs.size();i++) {
			sum=Bytes.add(sum, writeString(strs.get(i))) ;
		}
		return sum;
	}
	
	public static byte[] writeString(String content) {
		byte[] tmp = Bytes.toBytes(content);
		int len = tmp.length;
		return Bytes.add(Bytes.toBytes(len), tmp);
	}
}
