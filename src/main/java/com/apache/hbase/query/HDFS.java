package com.apache.hbase.query;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * hdfs工具类，删除文件
 * @author zhangfeng
 *
 */
public class HDFS {
	
	/**
	 * 删除HDFS上的文件
	 * @param hdfsPath 要删除的hdfs上的文件路径
	 * @throws IOException
	 */
	public static void deleteFileDir(String hdfsPath)
			throws IOException {
		Configuration conf = new Configuration();
		//这个地方skyform替换成自己core-site.xml文件中的fs.defaultFS向对应的值
	    FileSystem fs=FileSystem.get(URI.create(hdfsPath),conf);  
		Path dstPath = new Path(hdfsPath);
		fs.delete(dstPath, true);
	}
	/**
	 * 下载HDFS上的文件到本地
	 * @param hdfsPath HDFS文件路径
	 * @param localFilePath 本地存放路径
	 * @throws IOException
	 */
	public static void downFileToLocal(String hdfsPath, String localFilePath)
			throws IOException {
		Configuration conf = new Configuration();
		//这个地方skyform替换成自己core-site.xml文件中的fs.defaultFS向对应的值
	    FileSystem fs=FileSystem.get(URI.create(hdfsPath),conf);  
		Path dstPath = new Path(hdfsPath);
		FSDataInputStream in = fs.open(dstPath);
		OutputStream out = new FileOutputStream(new File(localFilePath));
		IOUtils.copy(in, out);
	}
	
	public static void main(String args[]) throws IOException{
		//这里是你的要在的HDFS上的额文件路径
		String hdfsPath = "hdfs://julong/data/0028_0.txt";
		String localPath = "d:\\0028_0.txt";
		downFileToLocal(hdfsPath,localPath);
	}
	

}
