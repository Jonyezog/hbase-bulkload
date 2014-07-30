package com.apache.hbase.query;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
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
	
	private String timeout;
	
	private static Configuration conf = null;
	

	
	public HBaseQuery(){
		
		this.quorum = PropertiesHelper.getInstance().getValue("hbase.zookeeper.quorum");
		this.port = Integer.parseInt(PropertiesHelper.getInstance().getValue("hbase.zookeeper.property.clientPort"));
		this.znodeParent = PropertiesHelper.getInstance().getValue("zookeeper.znode.parent");
		this.tablePrefix = PropertiesHelper.getInstance().getValue("hbase.table.prefix");
		this.timeout = PropertiesHelper.getInstance().getValue("hbase.rpc.timeout");
		
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
		HBaseAdmin hbaseadmin = null;
		try{
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
					Thread thread = new Thread(new QueryThread(manager,tableName,results,query,quorum,this.port,this.znodeParent));
					thread.start();
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(hbaseadmin != null ){
				hbaseadmin.close();
			}
		}
	}

	private void convertMap(NavigableMap<HRegionInfo, ServerName> regions,Map<String,HRegionInfo> infos){
		for(HRegionInfo region :regions.keySet()){
			infos.put(regions.get(region).getServerName(), region);
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
		HBaseAdmin hbaseadmin = null;
		HTable table = null;
		try{
			Map<String,HRegionInfo> infos = new HashMap<String,HRegionInfo>();
			String tableName = "";
			for(String date : scopes){
				String[] datas = date.split("#");
				String start = datas[0];
				Date day = format.parse(start);
				String tn = this.tablePrefix + format1.format(day) ;
				tableName += tn +",";
				
				hbaseadmin = new HBaseAdmin(conf);
				//如果表不存在
				if(!hbaseadmin.tableExists(tn)){
					LOG.info(tableName + "表未找到");
					manager.setStatus(tableName, true);
				} else {
					table = new HTable(conf, tn);
					NavigableMap<HRegionInfo, ServerName> regions = table.getRegionLocations();
					convertMap(regions,infos);
				}
			}
			LOG.info("tableName : " + tableName);
			LOG.info("NavigableMap : " + infos);
			//启动查询线程
			for (String serverName : infos.keySet()) {
				manager.setStatus(serverName, false);
				HRegionInfo region = infos.get(serverName);
				String tn = region.getTable().getNameAsString();
				Thread thread = new Thread(new CoprocessorQueryThread(tn,conf,manager,tableName,results,query,region,serverName));
				thread.start();				
			}
	
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(hbaseadmin != null ){
				hbaseadmin.close();
			}
		}
		
	}
	
	/**
	 * 获取表中的记录数
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	public TableRecord getRecordForTable(String tableName) throws IOException{
		HTableInterface table = null;
		try{
			table = new HTable(conf,"FSN_TOTAL");
			Get get = new Get(Bytes.toBytes(tableName));
			Result result = table.get(get);
			String totalRecord = "0";
			String invalRecord = "0";
			String errorRecord = "0";
			if(result != null && !result.isEmpty()){
				totalRecord = new String(result.getValue("cf".getBytes(), "totalRecord".getBytes()));
				invalRecord = new String(result.getValue("cf".getBytes(), "invalRecord".getBytes()));
				errorRecord = new String(result.getValue("cf".getBytes(), "errorRecord".getBytes()));
			}
			TableRecord record = new TableRecord();
			record.setErrorRecord(errorRecord);
			record.setTotalRecord(totalRecord);
			record.setInvalRecord(invalRecord);
			return record;
		} catch(Exception e){
			e.printStackTrace();
		}finally{
			if(table != null){
				table.close();
			}
		}
		return null;
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

				
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = new Date();
		date.setTime(1404176406000l);
		System.out.println(format.format(date));
		String start = "2014-07-01 08:56:02";
		String end = "2014-07-29 00:43:38";
		String gzh = "UBYL";//冠字号至少要输入前四位字母
		QueryObject object = new QueryObject();
		object.setEnd(end);
		object.setGzh(gzh);
		object.setFr("45");
		object.setQy("17");
		object.setCzr("");//操作人
		object.setSbbm("");//设备编码
		object.setWd("0045");//网点
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
	
	
	public String selectByRowkeyRange(String tableName, String startRowkey,
			String endRowkey,QueryObject object) throws IOException {
		HTable table = null;
		StringBuffer buffer = new StringBuffer();
		HBaseAdmin hbaseadmin = null;
		try{
			hbaseadmin = new HBaseAdmin(conf);
			//如果表不存在
			if(!hbaseadmin.tableExists(tableName)){
				LOG.info(tableName + " table not found!");
				return null;
			} else {
				
				table = new HTable(conf, tableName);
				Map<String,HRegionInfo> regionInfos = new HashMap<String,HRegionInfo>();
				NavigableMap<HRegionInfo, ServerName> regions = table.getRegionLocations();
				for(HRegionInfo region :regions.keySet()){
					regionInfos.put(regions.get(region).getServerName(), region);
				}
				
				for(String region :regionInfos.keySet()){
					System.out.println(region);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}	finally{
			if(table != null){
				table.close();
			}
		}
		return null;
	}
	
	private String addZeroForNum(String str, int strLength,String ch) {
	     int strLen = str.length();
	     StringBuffer sb = null;
	     while (strLen < strLength) {
	           sb = new StringBuffer();
	           sb.append(str).append(ch);
	           str = sb.toString();
	           strLen = str.length();
	     }
	     return str;
	 }	
	
	private boolean isEmpty(String value){
		if(value != null && !value.equals("")){
			return false;
		}
		return true;
	}	
}
