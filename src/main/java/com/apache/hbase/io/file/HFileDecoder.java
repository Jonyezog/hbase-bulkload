package com.apache.hbase.io.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.net.URI;
public class HFileDecoder {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		conf.set("io.compression.codecs","org.apache.hadoop.io.compress.SnappyCodec");
		FileSystem fs = FileSystem.get(URI.create("hdfs://dp22:8020/"),conf);
		Path path = new Path("/apps/hbase/data/data/default/t1/850c8d49400671e99b6208aa40509b66/f1/d7cd2cc82c0b4dd5b54ab2bc95701160");
		HFile.Reader reader = HFile.createReader(fs, path,new CacheConfig(conf),conf);
		reader.loadFileInfo();
		HFileScanner scanner = reader.getScanner(false, false);
		scanner.seekTo();
		int count = 1;
		while(scanner.next()){
			KeyValue keyvalue = scanner.getKeyValue();
			System.out.println(Bytes.toString(keyvalue.getFamily())+"-->"+Bytes.toString(keyvalue.getQualifier())+"-->"+Bytes.toString(keyvalue.getRow())+"-->"+Bytes.toString(keyvalue.getValue())+"");
			count ++;
		}
		System.out.println("count : " + count);
//		HFile.Reader.Scanner scanner = new HFile.Reader.Scanner(reader, false, false);
		byte[] firstRowKey = reader.getFirstRowKey();
//		System.out.println(Bytes.toString(firstRowKey));
		reader.close();

	}

}
