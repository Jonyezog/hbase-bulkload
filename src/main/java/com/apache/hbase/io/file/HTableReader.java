package com.apache.hbase.io.file;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;

public class HTableReader extends RecordReader<Text ,Text>{

    private LineReader lr ;  
    private Text key = new Text();  
    private Text value = new Text();  
    private long start ;  
    private long end;  
    private long currentPos;  
    private Text line = new Text();  
	
	@Override
	public void close() throws IOException {
		
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return null;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return null;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return false;
	}

}
