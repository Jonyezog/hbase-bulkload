package com.apache.hbase.bulkimport;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import com.apache.hbase.query.HDFS;

/**
 * HBase bulk import<br>
 * Data preparation MapReduce job driver
 * <ol>
 * <li>args[0]: HDFS input path
 * <li>args[1]: HDFS output path
 * <li>args[2]: HBase table name
 * </ol>
 */
@SuppressWarnings("deprecation")
public class Driver {
	private static final Logger LOG = Logger.getLogger(Driver.class);
	

	/**
	 * 创建表
	 * @param conf
	 * @param args
	 * @throws Exception
	 */
	private static void createTable(Configuration conf,String args[]) throws Exception{
		HBaseAdmin admin = new HBaseAdmin(conf);
		//判断表是否存在，如果不存在创建表，如果存在直接插入数据
		if(!admin.tableExists(args[2])){
			//set region startkey and endkey
			int numRegions = Integer.parseInt(args[8]);
			String startKey = args[6];
			String endKey = args[7];
			//column family
			HColumnDescriptor cf = new HColumnDescriptor("cf");
			//set compression type
			cf.setCompactionCompressionType(Compression.Algorithm.SNAPPY);
			cf.setCompressionType(Compression.Algorithm.SNAPPY);
			//htable desc
			HTableDescriptor td = new HTableDescriptor( args[2]); 
			td.addFamily(cf);
			admin.createTable(td, startKey.getBytes(), endKey.getBytes(), numRegions);
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		// HBase Configuration
		Configuration config = new Configuration();
		// 创建一个Job

		Job job = new Job(config,"HBase Bulk Import Data ,table name : " + args[2]);

		job.setJarByClass(HBaseKVMapper.class);
		job.setMapperClass(HBaseKVMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);
		TableMapReduceUtil.initTableReducerJob(args[2], null, job);

		job.setInputFormatClass(TextInputFormat.class);
		// 创建HBase的配置对象
		Configuration hbaseconfig = HBaseConfiguration.create();
		hbaseconfig.set("hbase.zookeeper.quorum", args[3]);
		hbaseconfig.set("hbase.zookeeper.property.clientPort", args[4]);
		hbaseconfig.set("zookeeper.znode.parent", "/" + args[5]);

		//创建Hbase表，压缩方式是snappy
		createTable(hbaseconfig,args);
		// 构造HTable对象
		HTable hTable = new HTable(hbaseconfig, args[2]);

		// 自动配置partitioner and reducer
		HFileOutputFormat.configureIncrementalLoad(job, hTable);

		// 设置文件的输入输出路径
		String[] files = args[0].split(",");
		for (String file : files) {
			FileInputFormat.addInputPath(job, new Path(file));
		}
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 等待Hfile文件生成完成
		job.waitForCompletion(true);

		Counters counters = job.getCounters();
		Counter separatorError = counters.findCounter("HBaseKVMapper", "PARSE_ERRORS");
		Log.info("Separator error record number :" + separatorError.getValue());
		Counter fieldError = counters.findCounter("HBaseKVMapper", "INVALID_FIELD_LEN");
		Log.info("Field error record number :" + fieldError.getValue());
		Counter records = counters.findCounter("HBaseKVMapper", "INVALID_FIELD_LEN");
		Log.info("Normal record number : " + records.getValue());

		// 装载hfile文件到HBase表中
		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(hbaseconfig);
		loader.doBulkLoad(new Path(args[1]), hTable);
		
		//删除输入的数据源
		Log.info("delete hdfs file ：" + args[0]);
		for (String file : files) {
			HDFS.deleteFileDir(file);
		}
		Log.info("delete hdfs file success!");


	}
}