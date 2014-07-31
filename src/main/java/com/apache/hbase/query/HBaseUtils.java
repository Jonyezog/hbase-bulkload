package com.apache.hbase.query;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.log4j.Logger;

import com.apache.hbase.query.util.PropertiesHelper;

/**
 * hbase操作工具类，主要提供删表操作，同时删除hdfs上的zip文件
 * @author zhangfeng
 *
 */
public class HBaseUtils {

	private static final Logger LOG = Logger.getLogger(HBaseUtils.class);
	
	//zookeeper 地址
	private static String quorum ;
	//zk port
	private static String port;
	//hbase在zk上注册的根节点名称
	private static String znodeParent ;
	//表前缀
	private String tablePrefix;
	//zip文件在hdfs上的路径前缀
	private String hdfsPrefix;
	
	private static Configuration conf;
	
	private static HBaseAdmin hbaseAdmin;
	
	private HBaseUtils(){
		quorum = PropertiesHelper.getInstance().getValue("hbase.zookeeper.quorum");
		port = PropertiesHelper.getInstance().getValue("hbase.zookeeper.property.clientPort");
		znodeParent = PropertiesHelper.getInstance().getValue("zookeeper.znode.parent");
		this.tablePrefix = PropertiesHelper.getInstance().getValue("hbase.table.prefix");
		this.hdfsPrefix = PropertiesHelper.getInstance().getValue("hdfs.prefix");
	}

	private static HBaseUtils hbase;
	
	public static HBaseUtils getInstance(){
		if(hbase == null ){
			hbase = new HBaseUtils();
		}
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", quorum);
		conf.set("hbase.zookeeper.property.clientPort", port);
		conf.set("zookeeper.znode.parent", "/" +znodeParent);
		try {
			hbaseAdmin = new HBaseAdmin(conf);
		} catch (Exception e) {
			e.printStackTrace();
		} 
		return hbase;
	}
	
	
	/**
	 * 删除表
	 * @param 天，删除那一天的数据，格式yyyyMMdd
	 * @return
	 */
	public boolean deleteTable(String day) {
		try {
			String tableName = this.tablePrefix + day;
			//如果表不存在
			if(!hbaseAdmin.tableExists(tableName)){
				LOG.info(tableName + " not exist，Cann't delete");
				return true;
			}
			//删除表
			hbaseAdmin.disableTable(tableName);
			hbaseAdmin.deleteTable(tableName);
			LOG.info(tableName + " delete success!");
			HTable table = new HTable(conf, "FSN_TOTAL");  
	        Delete del = new Delete(tableName.getBytes());  
	        table.delete(del);  
	        LOG.info("delete ["+tableName+"] record success!");
			//删除HDFS上的额zip文件
			HDFS.deleteFileDir(this.hdfsPrefix + day);
			LOG.info("[" +this.hdfsPrefix + day + "] dir delete success!");
			return true;
		} catch (Exception e) {
			LOG.error(e.getMessage());
			e.printStackTrace();
			return false;
		}
	}
	
}
