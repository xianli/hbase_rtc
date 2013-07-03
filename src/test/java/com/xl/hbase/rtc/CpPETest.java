package com.xl.hbase.rtc;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import static com.xl.hbase.rtc.expression.ExpressionFactory.*;

import com.xl.hbase.rtc.expression.*;
import com.xl.hbase.rtc.model.ResultSet;
import com.xl.hbase.rtc.service.Client;

public class CpPETest extends Thread{
	Client client=Client.instance("vmdev40");
	private String tableName;
	private int regionNum;
	private int rowsPerRegion;
	private int regionSize;
	private DataInit di;
	
	/**
	 * Constructor 
	 * @param di
	 */		
	public CpPETest(DataInit di){
		tableName=di.getTableName();
		if(!di.isIS_TYPE_OF_REGIONSIZE()){
			regionNum=di.getRegionNum();
			rowsPerRegion=di.getRowsPerRegion();
		}else{
			regionSize=di.getRegionSize();
		}
	}
	
	/**
	 * Coprocessor method
	 * @param startkey int for scan 
	 * @param endkey int for scan
	 * @return
	 */
	public  long testSelect(int startkey,int endkey){
		long ctime=System.currentTimeMillis();
		Select selectCol = select("d:name","d:age",
				"d:salary");
//		Where whereCondition1 =  eq("d:name", "xianli");
		List<OrderBy> orderbys = aslist(asc("d:name"));
		try {
			ResultSet rs = client.simpleQuery(Bytes.toBytes(startkey), Bytes.toBytes(endkey), 
					selectCol, tableName, null, orderbys,100);		
			long endtime=System.currentTimeMillis()-ctime;
			return endtime;
			} catch (Throwable e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	/**
	 * Native method
	 * @param startkey int for scan 
	 * @param endkey int for scan
	 * @return
	 */
	public long testSelectStub(int startkey,int endkey){
		long ctime=System.currentTimeMillis();
		Select selectCol = select("d:name","d:age",
				"d:salary");
		Where whereCondition1 =  eq("d:name", "xinrong");
//		List<OrderBy> orderbys = aslist(asc("d:salary"));
		try {
			ResultSet rs = client.simpleQueryStub(Bytes.toBytes(startkey), Bytes.toBytes(endkey), 
					selectCol, tableName, whereCondition1, null, Client.NOTOP);
			long endtime=System.currentTimeMillis()-ctime;
			return endtime;
		} catch (Throwable e) {
			e.printStackTrace();
		}
	return 0;
	}
	
	/**
	 * Coprocessor method
	 * @param startkey int for scan 
	 * @param endkey int for scan
	 * @return
	 */
	public long testAggSelect(int startkey,int endkey){
		AggSelect aggselect = select(as(avg("d:age"),"age"),
				as(avg("d:salary"),"AVGsalary"));
		GroupBy groupbys = groupby("d:name");

//		Where whereCondition1 =  eq("_row_3", 20120618);	
//		List<OrderBy> orderbys = aslist(asc("d:name"));
		try {
			long ctime=System.currentTimeMillis();
			ResultSet rs = client.aggregateQuery(Bytes.toBytes(startkey), Bytes.toBytes(endkey), 
					aggselect, tableName, null, groupbys, null, null, Client.NOTOP);
			rs.print(System.out);
			long endtime=System.currentTimeMillis()-ctime;
			return endtime;
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return Long.MAX_VALUE/2;
	}
	
	/**
	 * Native method
	 * @param startkey int for scan 
	 * @param endkey int for scan
	 * @return
	 */
	public long testAggSelectStub(int startkey,int endkey){
		AggSelect aggselect = select(as(avg("d:age"),"age"),
				as(avg("d:salary"),"AVGsalary"));
		GroupBy groupbys = groupby("d:hoppy");

//		Where whereCondition1 =  eq("_row_3", 20120618);	
//		List<OrderBy> orderbys = aslist(asc("d:UPV"), asc("_row_2"));
		try {
			long ctime=System.currentTimeMillis();
			ResultSet rs = client.aggregateQueryStub(Bytes.toBytes(startkey), Bytes.toBytes(endkey), 
					aggselect, tableName, null, groupbys, null, null, Client.NOTOP);
			rs.print(System.out);
			long endtime=System.currentTimeMillis()-ctime;
			return endtime;
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return Long.MAX_VALUE/2;
	}
	
	/**
	 * 根据输入参数执行对select及aggregate的测试，返回各种测试的测试类型及均值
	 * @param span 测试的行数范围
	 * @param times 每个范围的测试次数，自动求均值
	 * @return
	 */
	public HashMap runTest(int [] span,int times){
		HashMap <String,Long> hs=new HashMap<String,Long>();
		for(int j=0;j<span.length;j++){
			long co=0,nat=0;
			int count=0;
			for(int i=0;i<3;i++){
				int start=9700,end=span[j]+start;
				co+=testSelect(start, end);
//				nat+=testAggSelect(start, end);
				count++;
			}
			hs.put("Select\t"+span[j], co/count);
//			hs.put("Aggregate\t"+span[j], nat/count);
		}
		return hs;
	}
	
	/**
	 * 根据输入参数执行对select及aggregate的测试，返回各种测试的测试类型及均值
	 * @param span 测试的行数范围
	 * @param times 每个范围的测试次数，自动求均值
	 * @return
	 */
	public HashMap runTestStub(int [] span,int times){
		HashMap <String,Long> hs=new HashMap<String,Long>();
		for(int j=0;j<span.length;j++){
			long co=0,nat=0;
			int count=0;
			for(int i=0;i<3;i++){
				int start=0,end=span[j];
				co+=testSelectStub(start, end);
				nat+=testAggSelectStub(start, end);
				count++;
			}
			hs.put("SelectStub\t"+span[j], co/count);
			hs.put("AggregateStub\t"+span[j], nat/count);
		}
		return hs;
	}
	
	
	public static void main(String...args) throws IOException{		
		DataInit di=new DataInit(10,10000);
		CpPETest cp=new CpPETest(di);
		int []span={1,10,100,300,500,1000,5000,10000,30000,50000,100000};//测试的行数范围
		int []spanstub={100000};//测试的行数范围

		HashMap<String,Long> hs=cp.runTest(spanstub,3);//输入行数范围和测试次数限制
		
		System.out.println("Range of Span\t  UseTime");
		for(Map.Entry<String, Long> en:hs.entrySet()){
			System.out.println(en.getKey()+"\t\t"+en.getValue());
		}
	}
}
