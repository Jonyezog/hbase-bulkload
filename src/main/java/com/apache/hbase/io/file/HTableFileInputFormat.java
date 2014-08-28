package com.apache.hbase.io.file;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class HTableFileInputFormat extends FileInputFormat<Text, Text> {

	@Override
	public RecordReader<Text, Text> getRecordReader(InputSplit arg0,
			JobConf arg1, Reporter arg2) throws IOException {
		
		return null;
	}

}
