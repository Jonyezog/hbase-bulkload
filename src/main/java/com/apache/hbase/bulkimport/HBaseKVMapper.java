package com.apache.hbase.bulkimport;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Mapper Class
 */
public class HBaseKVMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
	// 列簇名称
	final static byte[] SRV_COL_FAM = "cf".getBytes();
	// 数据字段数量
	final static int NUM_FIELDS = 15;


	ImmutableBytesWritable hKey = new ImmutableBytesWritable();
	KeyValue kv;

	/** {@inheritDoc} */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration c = context.getConfiguration();
	}

	/** {@inheritDoc} */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] fields = null;

		// 解析字段，如果错误记录错误数量
		try {
			fields = value.toString().split(",");
		} catch (Exception ex) {
			context.getCounter("HBaseKVMapper", "PARSE_ERRORS").increment(1);
			return;
		}

		// 如果字段数量不正确，记录字段数量不正确的记录数
		if (fields.length != NUM_FIELDS) {
			context.getCounter("HBaseKVMapper", "INVALID_FIELD_LEN").increment(1);
			return;
		}
		//设置rowkey(冠字号+时间)
		String rowkey = fields[11] + fields[0];
//		String rowkey = fields[9] + fields[0];
		hKey.set(rowkey.getBytes());
		// Save KeyValue Pair
		kv = new KeyValue(hKey.get(), SRV_COL_FAM,HColumnEnum.SRV_COL_B.getColumnName(), value.toString().getBytes());
		// Write KV to HBase
		context.write(hKey, kv);
		context.getCounter("HBaseKVMapper", "NUM_MSGS").increment(1);
	}
}
