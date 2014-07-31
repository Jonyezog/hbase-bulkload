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

/**
 * 对外统一提供的数据查询接口
 * @author zhangfeng
 *
 */
public class HBaseQuery implements Query{
	
	private static final Logger LOG = Logger.getLogger(HBaseQuery.class);
	//zookeeper 地址
	private String quorum ;
	//zk port
	private int port;
	
	//hbase在zk上注册的根节点名称
	private String znodeParent ;
	//hbase的表前缀
	private String tablePrefix ;
	
	//hbase配置信息
	private static Configuration conf = null;
	
	public HBaseQuery(){
		this.quorum = PropertiesHelper.getInstance().getValue("hbase.zookeeper.quorum");
		this.port = Integer.parseInt(PropertiesHelper.getInstance().getValue("hbase.zookeeper.property.clientPort"));
		this.znodeParent = PropertiesHelper.getInstance().getValue("zookeeper.znode.parent");
		this.tablePrefix = PropertiesHelper.getInstance().getValue("hbase.table.prefix");
		
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", this.quorum);
		conf.set("hbase.zookeeper.property.clientPort", this.port+"");
		conf.set("zookeeper.znode.parent", "/" +this.znodeParent);
	}



	/**
	 * 根据开始和结束日止，获取中间的每一天查询的开始和结束时间，开始和结束时间之间使用#隔开，例如:2014-07-01 00:23:12#2014-07-01 23:59:59
	 * @param start
	 * @param end
	 * @return
	 * @throws ParseException
	 */
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
	private void exactMatch(QueryObject query,QueryStatusManager manager) throws Exception {
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
				//生成表名
				String tableName = this.tablePrefix + format1.format(day);
				hbaseadmin = new HBaseAdmin(conf);
				//如果表不存在
				if(!hbaseadmin.tableExists(tableName)){
					LOG.info(tableName + "表未找到");
					//如果表不存在，设置该表的查询状态为完成
					manager.setStatus(tableName, true);
				} else {
					manager.setStatus(tableName, false);
					//启动查询线程
					Thread thread = new Thread(new QueryThread(manager,tableName,query,quorum,this.port,this.znodeParent));
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

	/**
	 * 获取每张表的其中一个region所在的region server名称，并建立和region的对应关系
	 * @param regions
	 * @param infos
	 */
	private void convertMap(NavigableMap<HRegionInfo, ServerName> regions,Map<String,HRegionInfo> infos){
		for(HRegionInfo region :regions.keySet()){
			String tableName = region.getTable().getNameAsString();
			infos.put(regions.get(region).getServerName()+ "@" + tableName, region);
		}
	}
	
	
	/**
	 * 模糊匹配查询
	 * @param query
	 * @param manager
	 * @param results
	 * @throws Exception
	 */
	private void fuzzyMatch(QueryObject query,QueryStatusManager manager) throws Exception {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat format1 = new SimpleDateFormat("yyyyMMdd");
		//获取查询条件的时间范围区间
		List<String> scopes = generalScope(query.getStart(),query.getEnd());
		HBaseAdmin hbaseadmin = null;
		HTable table = null;
		try{
			Map<String,HRegionInfo> infos = new HashMap<String,HRegionInfo>();
			String tableName = "";
			for(String date : scopes){
				//获取开始时间
				String[] datas = date.split("#");
				String start = datas[0];
				Date day = format.parse(start);
				//表名
				String tn = this.tablePrefix + format1.format(day) ;
				tableName += tn +",";
				
				hbaseadmin = new HBaseAdmin(conf);
				//如果表不存在
				if(!hbaseadmin.tableExists(tn)){
					LOG.info(tableName + "表未找到");
					manager.setStatus(tableName, true);
				} else {
					table = new HTable(conf, tn);
					//获取表region的location信息
					NavigableMap<HRegionInfo, ServerName> regions = table.getRegionLocations();
					convertMap(regions,infos);
				}
			}
			//启动查询线程，每张表在其其中一个region所在的region server上启动一个查询线程
			for (String serverName : infos.keySet()) {
				//设置线程的查询状态为false，没有完成，线程的查询状态改变交给线程自己处理
				manager.setStatus(serverName, false);
				HRegionInfo region = infos.get(serverName);
				String tn = region.getTable().getNameAsString();
				//启动查询线程
				Thread thread = new Thread(new CoprocessorQueryThread(tn,conf,manager,tableName,query,serverName));
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
	 * 根据表名获取该表中的记录总数
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	public TableRecord getRecordForTable(String tableName) throws IOException{
		HTableInterface table = null;
		try{
			//创建htable对象
			table = new HTable(conf,"FSN_TOTAL");
			//通过表名get该表名对应的rowkey在表中的记录
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
		String gzh = query.getGzh();
		QueryStatusManager manager = new QueryStatusManager();
		manager.clear();
		try{
			//冠字号精确匹配查询,使用Rowkey范围过滤查询
			if(gzh != null && gzh.length() == 10){
				exactMatch(query,manager);
			} else {
				//否则如果冠字号小于10位，但是要大于4位，也按照范围查询
				this.fuzzyMatch(query, manager);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		while(!manager.isCompleted()){
			
		}
		LOG.info("query completed");
		LOG.info("query total count :" + manager.getResults().size());
		return manager.getResults();
	}	

}
