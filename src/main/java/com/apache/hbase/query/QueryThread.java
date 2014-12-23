package com.apache.hbase.query;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.SimpleDateFormat;


/**
 * 精确查询线程
 * @author zhangfeng
 *
 */
public class QueryThread implements Runnable {
	
	private static final Logger LOG = Logger.getLogger(QueryThread.class);
	
	private static Configuration conf = null;
	//查询状态管理器
	private QueryStatusManager manager;
	
	//表名
	private String tableName;
	
	//查询对象
	private QueryObject query;
	
	private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	//zk服务器地址
	private String quorums;
	//zk 端口
	private int port;
	//zk在服务器上注册的根节点名称
	private String znodeParent;


	public QueryThread(QueryStatusManager manager,String tableName,
			QueryObject query,final String quorums, final int port, final String znodeParent){
		this.manager = manager;
		this.tableName = tableName;
		this.query = query;
		this.quorums = quorums;
		this.port = port;
		this.znodeParent = znodeParent;	
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", this.quorums);
		conf.set("hbase.zookeeper.property.clientPort", this.port+"");
		conf.set("zookeeper.znode.parent", "/" +this.znodeParent);
			
	}
	
	public void run() {
		try {
			long starttime = format.parse(query.getStart()).getTime() / 1000;
			long endtime = format.parse(query.getEnd()).getTime() / 1000;
			//生成开始和结束rowkey
			String startRowkey = query.getGzh() + starttime;
			String endRowkey = query.getGzh() + endtime;
			//执行查询
			selectByRowkeyRange(this.tableName,startRowkey,endRowkey);
			//设置线程的查询状态为完成
			this.manager.setStatus(tableName, true);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	
	/**
	 * 根据表名，开始key，结束key查询数据
	 * @param tableName
	 * @param startRowkey
	 * @param endRowkey
	 * @return
	 * @throws IOException
	 */
	public void selectByRowkeyRange(String tableName, String startRowkey,
			String endRowkey) throws IOException {
		HTableInterface table = null;
		try{
			table = new HTable(conf,tableName);
			Scan scan = new Scan();
			scan.setStartRow(startRowkey.getBytes());
			//设置scan的扫描范围由startRowkey开始
			Filter filter =new InclusiveStopFilter(endRowkey.getBytes());
			scan.setFilter(filter);
			//设置scan扫描到endRowkey停止，因为setStopRow是开区间，InclusiveStopFilter设置的是闭区间
			ResultScanner rs = table.getScanner(scan);
			int count = 0;
			for (Result r : rs) {
				boolean frRight = true;
				boolean qyRight = true;
				boolean sbbmRight = true;
				boolean czrRight = true;
				boolean wdRight = true;
				String value = new String(r.getValue("cf".getBytes(), "c1".getBytes()));
				String[] datas = value.split(",");
				//如果法人条件不为空，并且数据中的法人和查询条件中的值不一致，结果为false
				if(!isEmpty(query.getFr()) && !query.getFr().equals(datas[2])){
					frRight = false;
				}
				//如果区域条件不为空，并且数据中的区域和查询条件中的值不一致，结果为false
				if(!isEmpty(query.getQy()) && !query.getQy().equals(datas[1])){
					qyRight = false;
				}
					
				//如果设备编码条件不为空，并且数据中的设备编码和查询条件中的值不一致，结果为false
				if(!isEmpty(query.getSbbm()) && !query.getSbbm().equals(datas[4])){
					sbbmRight = false;
				}
				//如果操作人条件不为空，并且数据中的操作人和查询条件中的值不一致，结果为false
				if(!isEmpty(query.getCzr()) && !query.getCzr().equals(datas[5])){
					czrRight = false;
				}
				//如果网点条件不为空，并且数据中的网点和查询条件中的值不一致，结果为false
				if(!isEmpty(query.getWd()) && !query.getWd().equals(datas[3])){
					wdRight = false;
				}
				if(wdRight && frRight && sbbmRight && czrRight && qyRight){
					manager.getResults().add(value);
					count ++;
				}
			}
			LOG.info("table ["+ tableName +"] query record count :" + count);
		} catch (Exception e) {
			LOG.info("发生错误，无法查找");
			e.printStackTrace();
		}	finally{
			if(table != null){
				table.close();
			}
		}
		
	}
	
	private boolean isEmpty(String value){
		if(value != null && !value.equals("")){
			return false;
		}
		return true;
	}
	
	
}
