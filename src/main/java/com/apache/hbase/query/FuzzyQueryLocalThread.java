package com.apache.hbase.query;

import java.util.List;
import java.util.Map;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;

/**
 * 查询线程
 * 
 * @author zhangfeng
 * 
 */
public class FuzzyQueryLocalThread implements Runnable {

	private static Configuration conf = null;

	private QueryStatusManager manager;

	private String tableName;

	private List<String> results;

	private QueryObject query;

	private String quorums;

	private int port;

	private String znodeParent;
	
	Map<String,HRegionInfo> infos = null;

	public FuzzyQueryLocalThread(Map<String,HRegionInfo> infos,QueryStatusManager manager, String tableName,
			List<String> results, QueryObject query, final String quorums,
			final int port, final String znodeParent, final String timeout) {
		this.manager = manager;
		this.tableName = tableName;
		this.results = results;
		this.query = query;
		this.quorums = quorums;
		this.port = port;
		this.znodeParent = znodeParent;
		this.infos = infos;
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", this.quorums);
		conf.set("hbase.zookeeper.property.clientPort", this.port + "");
		conf.set("zookeeper.znode.parent", "/" + this.znodeParent);
		conf.set("hbase.rpc.timeout", timeout);
	}
	


	public void run() {
		try {
			for (String serverName : infos.keySet()) {
				this.manager.setStatus(serverName, false);
				HRegionInfo region = infos.get(serverName);
				String tn = region.getTable().getNameAsString();
				Thread thread = new Thread(new CoprocessorQueryThread(tn,conf,manager,tableName,results,query,region,serverName));
				thread.start();				
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
