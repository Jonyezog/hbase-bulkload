package com.apache.hbase.mr;


import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Zip格式的文件输入
 *
 * Created by zhangfeng on 2015/1/7.
 */
public class ZipFileInputFormat extends FileInputFormat<Text, BytesWritable> {

    /** See the comments on the setLenient() method */
    private static boolean isLenient = false;

    /**
     * 用来确定你是否可以切分一个块，默认返回为true，表示只要数据块大于hdfs block size，
     * 那么它将会被切分。但有时候你不希望切分一个文件，
     * 例如某些二进制序列文件不能被切分时，你就需要重载该函数使其返回false
     * 我们这里是zip文件，不能拆分，所以返回false
     */
    @Override
    protected boolean isSplitable( JobContext context, Path filename )
    {
        return false;
    }

    /**
     * Create the ZipFileRecordReader to parse the file
     */
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader( InputSplit split, TaskAttemptContext context )
            throws IOException, InterruptedException
    {
        return new ZipFileRecordReader();
    }

    /**
     *
     * @param lenient
     */
    public static void setLenient( boolean lenient )
    {
        isLenient = lenient;
    }

    public static boolean getLenient()
    {
        return isLenient;
    }
}
