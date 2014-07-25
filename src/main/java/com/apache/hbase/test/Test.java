package com.apache.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;

public class Test {
	private static final byte[] TABLE_NAME = Bytes.toBytes("FSN_20140723");
	private static final byte[] CF = Bytes.toBytes("cf");

	public static void main(String[] args) throws Throwable {
		Configuration configuration = HBaseConfiguration.create();

		configuration.set("hbase.zookeeper.quorum", "dp23,dp24,dp25");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("zookeeper.znode.parent", "/hbase-unsecure");
		HTable table = new HTable(configuration, "FSN_20140723");
		AggregationClient aggregationClient = new AggregationClient(configuration);
		Scan scan = new Scan();
		// 指定扫描列族，唯一值
		scan.addFamily(CF);
		long rowCount = aggregationClient.rowCount(table, new LongColumnInterpreter(), scan);
		System.out.println("row count is " + rowCount);

	}

}
