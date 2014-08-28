package com.apache.hbase.io.file;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import com.apache.hbase.query.util.PropertiesHelper;

public class PutTest {

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
	
	public PutTest(){
		this.quorum = PropertiesHelper.getInstance().getValue("hbase.zookeeper.quorum");
		this.port = Integer.parseInt(PropertiesHelper.getInstance().getValue("hbase.zookeeper.property.clientPort"));
		this.znodeParent = PropertiesHelper.getInstance().getValue("zookeeper.znode.parent");
		this.tablePrefix = PropertiesHelper.getInstance().getValue("hbase.table.prefix");
		
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", this.quorum);
		conf.set("hbase.zookeeper.property.clientPort", this.port+"");
		conf.set("zookeeper.znode.parent", "/" +this.znodeParent);
	}
	
	public static void main(String[] args) throws Exception {
		PutTest test = new PutTest();
		HTable htable = new HTable(conf, "t1");
		List<Put> puts = new ArrayList<Put>();
		for(int i = 0 ;i < 1000 ; i ++){
			Put put = new Put(UUID.randomUUID().toString().getBytes());
			String value = "1406044800,00,28,0000,1,0;0;0,CNY,9999,100,VEJF592592,CCB13/GLY/001060165,/20140723/00/0028/0028_0.zip,132";
			put.add("f1".getBytes(), "v".getBytes(), value.getBytes());
			puts.add(put);
		}
		htable.put(puts);
		htable.flushCommits();

	}

}
