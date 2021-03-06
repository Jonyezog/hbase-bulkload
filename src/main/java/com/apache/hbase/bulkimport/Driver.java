package com.apache.hbase.bulkimport;

import com.apache.hbase.query.HDFS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import java.io.IOException;

/**
 * HBase bulk import
 * Data preparation MapReduce job driver
 * <ol>
 * <li>args[0]: HDFS input path
 * <li>args[1]: HDFS output path
 * <li>args[2]: HBase table name
 * <li>args[3]: hbase.zookeeper.quorum
 * <li>args[4]: hbase.zookeeper.property.clientPort
 * <li>args[5]: zookeeper.znode.parent
 * <li>args[6]: 预查分hbase表region的startKey
 * <li>args[7]: 预查分hbase表region的endkey
 * <li>args[8]: 预查分hbase表region的数量
 * </ol>
 */
@SuppressWarnings("deprecation")
public class Driver {
	private static final Logger LOG = Logger.getLogger(Driver.class);

	public static enum MY_COUNTER {
		PARSE_ERRORS, INVALID_FIELD_LEN, NUM_MSGS
	};

	/**
	 * 创建hbase表，对表根据给定的startkey和endkey进行预分配region
	 * 
	 * @param conf
	 * @param args
	 * @throws Exception
	 */
	private static void createTable(Configuration conf, String args[])
			throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		// 判断表是否存在，如果不存在创建表，如果存在直接插入数据
		if (!admin.tableExists(args[2])) {
			//设置预拆分表的region数量，startkey，endkey
			int numRegions = Integer.parseInt(args[8]);
			String startKey = args[6];
			String endKey = args[7];
			//创建表的column family
			HColumnDescriptor cf = new HColumnDescriptor("cf");
			//设置数据的压缩方式为SNAPPY
			cf.setCompactionCompressionType(Compression.Algorithm.SNAPPY);
			cf.setCompressionType(Compression.Algorithm.SNAPPY);
			//创建表
			HTableDescriptor td = new HTableDescriptor(args[2]);
			td.addFamily(cf);
			admin.createTable(td, startKey.getBytes(), endKey.getBytes(),
					numRegions);
		}
	}

	public static void main(String[] args) throws Exception {
		// HBase Configuration
		Configuration config = new Configuration();
		// 创建一个Job

		Job job = new Job(config, "HBase Bulk Import Data ,table name : " + args[2]);

		job.setJarByClass(HBaseKVMapper.class);
		job.setMapperClass(HBaseKVMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);
		//初始化表的reducer数
		TableMapReduceUtil.initTableReducerJob(args[2], null, job);

		job.setInputFormatClass(TextInputFormat.class);
		// 创建HBase的配置对象
		Configuration hbaseconfig = HBaseConfiguration.create();
		hbaseconfig.set("hbase.zookeeper.quorum", args[3]);
		hbaseconfig.set("hbase.zookeeper.property.clientPort", args[4]);
		hbaseconfig.set("zookeeper.znode.parent", "/" + args[5]);

		// 创建Hbase表，压缩方式是snappy
		createTable(hbaseconfig, args);
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
		//获取job计数器中记录的数据
		Counters counters = job.getCounters();
		Counter separatorError = counters.findCounter(Driver.MY_COUNTER.PARSE_ERRORS);
		Log.info("Separator error record number :" + separatorError.getValue());
		Counter fieldError = counters.findCounter(Driver.MY_COUNTER.INVALID_FIELD_LEN);
		Log.info("Field error record number :" + fieldError.getValue());
		Counter records = counters.findCounter(Driver.MY_COUNTER.NUM_MSGS);
		Log.info("Normal record number : " + records.getValue());
		// 装载hfile文件到HBase表中
		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(hbaseconfig);
		loader.doBulkLoad(new Path(args[1]), hTable);

		//记录hbase每张表的记录总数
		createRecordTable(hbaseconfig,"FSN_TOTAL");
		String totalRecords = records.getValue() +"";
		String invalRecords = fieldError.getValue() +"";
		String errorRecords = separatorError.getValue() +"";
		insertData(hbaseconfig,args[2],totalRecords,invalRecords,errorRecords);
		// 删除输入的数据源
		Log.info("delete hdfs file ：" + args[0]);
		//删除txt文件
		for (String file : files) {
			HDFS.deleteFileDir(file);
		}
		Log.info("delete hdfs file success!");

	}
	
	/**
	 * 向Hbase标记录总数表中插入数据
	 * @param conf
	 * @param rowkey
	 * @param records
	 * @param invalRecord
	 * @param errorRecord
	 * @throws Exception
	 */
	private static void insertData(Configuration conf,String rowkey,String records,String invalRecord,String errorRecord) throws Exception{
		HTable table = null;
		Put put = new Put(Bytes.toBytes(rowkey));
		try {
			table = new HTable(conf,"FSN_TOTAL");
			Get get = new Get(Bytes.toBytes(rowkey));
			Result result = table.get(get);
			//如果是后续继续向表中插入数据，则更新该表的记录总数
			if(result != null && !result.isEmpty()){
				long tRecord = Long.parseLong(new String(result.getValue("cf".getBytes(), "totalRecord".getBytes())));
				long iRecord = Long.parseLong(new String(result.getValue("cf".getBytes(), "invalRecord".getBytes())));
				long eRecord = Long.parseLong(new String(result.getValue("cf".getBytes(), "errorRecord".getBytes())));
				tRecord = tRecord + Long.parseLong(records);
				iRecord = iRecord + Long.parseLong(invalRecord);
				eRecord = eRecord + Long.parseLong(errorRecord);
				records = Long.toString(tRecord);
				invalRecord = Long.toString(iRecord);
				errorRecord = Long.toString(eRecord);
			}			
			put.add(Bytes.toBytes("cf"), Bytes.toBytes("totalRecord"), Bytes.toBytes(records));
			put.add(Bytes.toBytes("cf"), Bytes.toBytes("invalRecord"), Bytes.toBytes(invalRecord));
			put.add(Bytes.toBytes("cf"), Bytes.toBytes("errorRecord"), Bytes.toBytes(errorRecord));
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(table != null){
				table.put(put);
				table.flushCommits();
				table.close();
			}
		}

	}
	
	/**
	 * 创建表
	 * 
	 * @param conf
	 * @throws Exception
	 */
	private static void createRecordTable(Configuration conf, String tableName)
			throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		// 判断表是否存在，如果不存在创建表，如果存在直接插入数据
		if (!admin.tableExists(tableName)) {
			HColumnDescriptor cf = new HColumnDescriptor("cf");
			// set compression type
			cf.setCompactionCompressionType(Compression.Algorithm.SNAPPY);
			cf.setCompressionType(Compression.Algorithm.SNAPPY);
			// htable desc
			HTableDescriptor td = new HTableDescriptor(tableName);
			td.addFamily(cf);
			admin.createTable(td);
		}
	}
	
}