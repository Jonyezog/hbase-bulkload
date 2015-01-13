package com.apache.hbase.mr;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

/**
 * Created by zhangfeng on 2015/1/7.
 */
public class ZipFileWordCount {
    private static final Logger log = Logger.getLogger(ZipFileWordCount.class);

    private final static int recordLength = 1644;
    
    public static class TokenizerMapper extends Mapper<Text, BytesWritable, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        	byte[] fsnBuffer = value.getBytes();
        	ByteBuffer byteBuffer = ByteBuffer.wrap(fsnBuffer);
			int readLen = 32;
			byte[] hb = new byte[readLen];
			byteBuffer.get(hb);
        	while(readLen < fsnBuffer.length){
        		byte[] bf = new byte[recordLength];
        		byteBuffer.get(bf, 0, recordLength);
        		FSNDecoder decoder = new FSNDecoder(bf);
        		System.out.println(decoder.getRecord());
        		readLen += recordLength;    
        		word.set(decoder.getRecord());
        		context.write(word, one);
        	}

 
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: ZipFileWordCount <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "Zip File Word Count");
        job.setJarByClass(ZipFileWordCount.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(ZipFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
