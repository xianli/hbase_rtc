package com.xl.hbase.rtc;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.jruby.threading.DaemonThreadFactory;

import com.xl.hbase.rtc.model.MetaData;
import com.xl.hbase.rtc.service.Client;
/**
 *
 * @author zhangxinrong
 * 
 * 为测试coprocessor接口性能造数据用的，使用说明见main，针对region大小的造数方法尚未确定。
 * 
 *	根据region的数量及每个region的row行数进行创建表及写入数据的操作
 */
public class DataInit {
	private final Logger logger=Logger.getLogger(DataInit.class);	
	private static HBaseAdmin admin;
	private static Configuration conf;
	private static HTablePool pool;
	private static Client cnt=Client.instance("vmdev40");
	
	private byte[]columFamily ="d".getBytes();
	private byte[]qualifier="test".getBytes();
	private String []qualifier_names={"name","age","salary","hoppy"};
	private boolean IS_TYPE_OF_REGIONSIZE=false;
	private String [] names={
			"xiaodong","xianli","fanxiong","cuirong","qunhe","hanfei","xinrong"
	};
	private String [] hobby={"startTrac,trival,films,football,E-games,reading,working,warcraft,counter strike","programming,coding,design,study"};
	private String tableName;
	private int regionNum;
	private int rowsPerRegion;
	private int regionSize;
	private ExecutorService threadpool;
	
	static{
		try {
			conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", "vmdev40");
			admin = new HBaseAdmin(conf);
			pool = new HTablePool(conf, 20);
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	/**
	 * constructor
	 */
	public DataInit(){
		this(1,1);
	}
	public DataInit(int regionSizeofGB){
		this.regionSize=regionSizeofGB*1024*1024;
		this.tableName="MadeByXinRong_RegSize_"+regionSizeofGB+"GB";
		this.IS_TYPE_OF_REGIONSIZE=true;
	}
	public DataInit(int regionNum,int rowPerRegion ){
		this.tableName="MadeByXinRong_"+rowPerRegion+"Per"+regionNum;
		this.regionNum=regionNum;
		this.rowsPerRegion=rowPerRegion;
		this.IS_TYPE_OF_REGIONSIZE=false;
	}
	
	
	public byte[] getColumFamily() {
		return columFamily;
	}
	public void setColumFamily(byte[] columFamily) {
		this.columFamily = columFamily;
	}
	public byte[] getQualifier() {
		return qualifier;
	}
	public void setQualifier(byte[] qualifier) {
		this.qualifier = qualifier;
	}
	public Logger getLogger() {
		return logger;
	}

	public boolean isIS_TYPE_OF_REGIONSIZE() {
		return IS_TYPE_OF_REGIONSIZE;
	}
	public String getTableName() {
		return tableName;
	}
	public int getRegionNum() {
		return regionNum;
	}
	public int getRowsPerRegion() {
		return rowsPerRegion;
	}
	public int getRegionSize() {
		return regionSize;
	}
	
	/**
	 * methods
	 * 
	 */
	public boolean creatTable(){
		try {
			if(admin.isTableAvailable(tableName)){
				return true;
			}
			if(this.IS_TYPE_OF_REGIONSIZE){
				this.conf.set("hbase.hregion.max.filesize", this.regionSize+"");
				HTableDescriptor dscp = new HTableDescriptor(tableName);
				dscp.addFamily(new HColumnDescriptor(Bytes.toBytes("d")));
				admin.createTable(dscp);
				logger.info("table "+tableName+" is created successfully");
			}else{
				byte[][]regionArray=new byte[regionNum-1][];
				for(int i=1;i<regionNum;i++){
					int temp=i*rowsPerRegion;
					regionArray[i-1]=Bytes.toBytes(temp);
				}
				HTableDescriptor dscp = new HTableDescriptor(tableName);
				dscp.addFamily(new HColumnDescriptor(columFamily));
				admin.createTable(dscp, regionArray);	
				logger.info(tableName+" is successed create");
			}
			if(admin.isTableDisabled(tableName)){
				admin.enableTable(tableName);
				Thread.sleep(1000);
			}
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("has not complete the creation of \""+tableName+"\", because the error:"+e);
		} 
		return false;
	}
	
	public boolean deletTable(){
        try {
			if (admin.isTableAvailable(tableName)) {
				if(admin.isTableEnabled(tableName)){
					admin.disableTable(tableName);
				    logger.info("table "+tableName+" has already disabled");
				}
				admin.deleteTable(tableName);
				logger.info("table "+tableName+" has been deleted");
			}else{
				logger.info("table "+tableName+" is not avaliable");
			}

				return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return false;
	}
	public void addCp(){
		cnt.addCoprocessor(tableName, "com.jd.hbase.rtc.coprocess.QueryImplementation");
		logger.info(tableName+" added the coprocessor successfully");
	}
	public void addMetaInfo(){
		MetaData md=new MetaData();
		md.setTableName(tableName);
		md.setRowkeyType("int");
		md.addColType("d:"+qualifier_names[0], "string");
		md.addColType("d:"+qualifier_names[1], "int");
		md.addColType("d:"+qualifier_names[2], "int");
		md.addColType("d:"+qualifier_names[3], "string");
		cnt.addMetaInfo(md);
		logger.info(tableName+" added to the _META_INFO successfully");
	}
	
	public boolean putData(){
		try {
			if(this.IS_TYPE_OF_REGIONSIZE){
				final HTable table=(HTable) pool.getTable(tableName);
				for(int i=0;i<1000;i++){
					Put put=new Put(Bytes.toBytes(i));
					put.add(columFamily, this.qualifier_names[0].getBytes(), Bytes.add(Bytes.toBytes(1), Bytes.add(Bytes.toBytes(names[i%6].getBytes().length), names[new Random().nextInt(7)].getBytes()))			);
					put.add(columFamily, this.qualifier_names[1].getBytes(), Bytes.add(Bytes.toBytes(1), Bytes.toBytes(10*new Random().nextInt(2)+24)));
					put.add(columFamily, this.qualifier_names[2].getBytes(), Bytes.add(Bytes.toBytes(1), Bytes.toBytes(10000*new Random().nextInt(7)+8000)));
					table.put(put);
				}
				table.close();
			}else{
				long start_time=System.currentTimeMillis();
				this.threadpool=new ThreadPoolExecutor(1,10,Long.parseLong("60"),TimeUnit.SECONDS,new SynchronousQueue<Runnable>(), new DaemonThreadFactory());
			    ((ThreadPoolExecutor)this.threadpool).allowCoreThreadTimeOut(true);
//				this.threadpool=Executors.newFixedThreadPool(10);
				for(int j=0;j<regionNum;j++){
					final int temp=j;
				    threadpool.submit(new Runnable(){
						@Override
						public void run() {
							final HTable table=(HTable) pool.getTable(tableName);
							logger.info("start put the region_"+(temp+1)+" of the Table:"+tableName);		
							try {
								for(int k=temp*rowsPerRegion;k<(temp+1)*rowsPerRegion;k++){	
									Put put=new Put(Bytes.toBytes(k));
									put.add(columFamily, qualifier_names[0].getBytes(), Bytes.add(Bytes.toBytes(1), Bytes.add(Bytes.toBytes(names[k%6].getBytes().length), names[k%6].getBytes())));
									put.add(columFamily, qualifier_names[1].getBytes(), Bytes.add(Bytes.toBytes(1), Bytes.toBytes(10*new Random().nextInt(2)+24)));
									put.add(columFamily, qualifier_names[2].getBytes(), Bytes.add(Bytes.toBytes(1), Bytes.toBytes(10000*new Random().nextInt(7)+8000)));
									put.add(columFamily, qualifier_names[3].getBytes(), Bytes.add(Bytes.toBytes(1), Bytes.add(Bytes.toBytes(hobby[k%2].getBytes().length), hobby[k%2].getBytes())));							
									table.put(put);
								}	
							table.close();
							} catch (IOException e) {								// TODO Auto-generated catch block
								logger.error("Put error",e);
							}	
						}
				    	
				    });
				}
				int time=0;
				threadpool.shutdown();
				while(!threadpool.awaitTermination(10, TimeUnit.SECONDS)){
					time+=10;
					if(time%60==0){
						System.out.println("the threadpool has not closed for "+time+" seconds");
					}
				}
				logger.info("total put time is "+(System.currentTimeMillis()-start_time));
			}

		} catch (Exception e) {
			logger.error(e);
		}
		return false;
	}
	
	public void initData(){
		this.creatTable();
		this.addCpAndMetaInfo();
		this.putData();
	}
	public void addCpAndMetaInfo(){
		this.addCp();
		this.addMetaInfo();
	}
	@Override
	public String toString(){
		return tableName;		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		DataInit di=new DataInit(3,10000);//初始时指定region的数量级每个region内的行数
//		di.deletTable();//若原表存在，可用于删除原表，
//		di.creatTable();//创建HTable表
//		di.addCp();//为该表增加coprocessor接口
//		di.addMetaInfo();//将该表加入到MetaInfo维表中
		di.putData();//在表中写入数据
////		System.out.println(di.deletTable());

	}
}
