package com.apache.hbase.io.file;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class HTableFileInputFormat extends FileInputFormat<Text, Text> {

	@Override
	public RecordReader<Text, Text> getRecordReader(InputSplit arg0,
			JobConf arg1, Reporter arg2) throws IOException {
		
		return null;
	}

}
