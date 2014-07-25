package com.apache.hbase.query;


import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HDFS {
	
	public static void deleteFileDir(String hdfsPath)
			throws IOException {
		Configuration conf = new Configuration();
		//这个地方skyform替换成自己core-site.xml文件中的fs.defaultFS向对应的值
	    FileSystem fs=FileSystem.get(URI.create(hdfsPath),conf);  
		Path dstPath = new Path(hdfsPath);
		fs.delete(dstPath, true);
	}

}
