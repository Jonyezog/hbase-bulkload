package com.apache.hbase.query;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;

import com.apache.hbase.query.util.PropertiesHelper;

public class HBaseQuery implements Query{
	
	private static final Logger LOG = Logger.getLogger(HBaseQuery.class);


	//zookeeper 地址
	private String quorum ;
	//zk port
	private int port;
	
	private String znodeParent ;
	
	private String tablePrefix ;
	
	private static Configuration conf = null;
	
	private HBaseAdmin hbaseadmin;
	
	public HBaseQuery(){
		
		this.quorum = PropertiesHelper.getInstance().getValue("hbase.zookeeper.quorum");
		this.port = Integer.parseInt(PropertiesHelper.getInstance().getValue("hbase.zookeeper.property.clientPort"));
		this.znodeParent = PropertiesHelper.getInstance().getValue("zookeeper.znode.parent");
		this.tablePrefix = PropertiesHelper.getInstance().getValue("hbas.table.prefix");
		
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", this.quorum);
		conf.set("hbase.zookeeper.property.clientPort", this.port+"");
		conf.set("zookeeper.znode.parent", "/" +this.znodeParent);
	}




	private List<String> generalScope(String start, String end) throws ParseException {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Date startDate = format.parse(start);
        Date endDate = format.parse(end);
        Calendar calendarTemp = Calendar.getInstance();
        calendarTemp.setTime(startDate);
        long newstart = calendarTemp.getTime().getTime();
        long et = endDate.getTime();
        
        List<String> scopes = new ArrayList<String>();
        while (newstart <= et) {
        	String date = format.format(calendarTemp.getTime()) ;
        	String startTime = "";
        	if(newstart == startDate.getTime()){
        		startTime = start;
        	} else {
        		startTime = date + " 00:00:00";
        	}
        	String endTime = "";
        	if(et == newstart){
        		endTime = end;
        	} else {
        		endTime = date + " 23:59:59";
        	}
            calendarTemp.add(Calendar.DAY_OF_YEAR, 1);
            newstart = calendarTemp.getTime().getTime();
            scopes.add(startTime + "#" + endTime);
        }
        return scopes;
    }
	
	/**
	 * 冠字号精确匹配查询
	 * @return
	 * @throws Exception 
	 */
	private void exactMatch(QueryObject query,QueryStatusManager manager,List<String> results) throws Exception {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat format1 = new SimpleDateFormat("yyyyMMdd");
		//获取时间每天的时间范围区间
		List<String> scopes = generalScope(query.getStart(),query.getEnd());
		for(String date : scopes){
			String[] datas = date.split("#");
			String start = datas[0];
			Date day = format.parse(start);
			String tableName = this.tablePrefix + format1.format(day);
			hbaseadmin = new HBaseAdmin(conf);
			//如果表不存在
			if(!hbaseadmin.tableExists(tableName)){
				LOG.info(tableName + "表未找到");
				manager.setStatus(tableName, true);
			} else {
				manager.setStatus(tableName, false);
				//启动查询线程
				Thread thread = new Thread(new QueryThread(manager,tableName,results,query,this.quorum,this.port,this.znodeParent));
				thread.start();
			}
		}
		
	}
	
	/**
	 * 模糊匹配查询
	 * @param query
	 * @param manager
	 * @param results
	 * @throws Exception
	 */
	private void fuzzyMatch(QueryObject query,QueryStatusManager manager,List<String> results) throws Exception {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat format1 = new SimpleDateFormat("yyyyMMdd");
		//获取时间每天的时间范围区间
		List<String> scopes = generalScope(query.getStart(),query.getEnd());
		//LOG.info("Query table count :" + scopes.size());
		for(String date : scopes){
			String[] datas = date.split("#");
			String start = datas[0];
			Date day = format.parse(start);
			String tableName = this.tablePrefix + format1.format(day);
			hbaseadmin = new HBaseAdmin(conf);
			//如果表不存在
			if(!hbaseadmin.tableExists(tableName)){
				LOG.info(tableName + "表未找到");
				manager.setStatus(tableName, true);
			} else {
				manager.setStatus(tableName, false);
				//启动查询线程
				Thread thread = new Thread(new FuzzyQueryThread(manager,tableName,results,query,this.quorum,this.port,this.znodeParent));
				thread.start();
			}
		}
		
	}
	
	
	
	
	
	/**
	 * 根据条件查询数据
	 * @param query 查询对象
	 * @return
	 */
	public List<String> query(QueryObject query) {
		List<String> results = new ArrayList<String>();
		String gzh = query.getGzh();
		QueryStatusManager manager = new QueryStatusManager();
		try{
			//冠字号精确匹配查询,使用Rowkey范围过滤查询
			if(gzh != null && gzh.length() == 10){
				exactMatch(query,manager,results);
			} else {//否则如果冠字号小于10位，但是要大于4位，也按照范围查询
				this.fuzzyMatch(query, manager, results);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		while(!manager.isCompleted()){
			//LOG.info("query is running... ");
		}
		LOG.info("query completed");
		LOG.info("query total count :" + results.size());
		return results;
	}	




	public static void main(String[] args) throws IOException, ParseException {
		HBaseQuery query = new HBaseQuery();
//		query.selectForResultScannerByRange("FSN_20140702", "XVZQ0000001404144147", "XVZQ9999991404576269");
//		//1404230400
//		//1404230547
//		//1404316799
//		query.scaneByPrefixFilter("FSN_20140711", "BSGW");
				
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = new Date();
		date.setTime(1406047418000l);
		System.out.println(format.format(date));
		String start = "2014-07-23 00:00:00";
		String end = "2014-07-23 00:43:38";
		String gzh = "HTCF7260";//冠字号至少要输入前四位字母
		QueryObject object = new QueryObject();
		object.setEnd(end);
		object.setGzh(gzh);
		object.setFr("28");
		object.setQy("00");
		object.setCzr("");//操作人
		object.setSbbm("");//设备编码
		object.setWd("");//网点
		object.setStart(start);
		Date t1 = new Date();
		List<String> list = query.query(object);
		for(String record : list){
			System.out.println(record);
		}
		Date t2 = new Date();
		System.out.println("end time : " + format.format(t2));
		System.out.println("total cost time : " + (t2.getTime() - t1.getTime()) / 1000 + " s");
//			
	}
}
