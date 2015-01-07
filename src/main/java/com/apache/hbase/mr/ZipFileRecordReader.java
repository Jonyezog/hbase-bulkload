package com.apache.hbase.mr;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 解压ZIp文件，读取zip压缩包中的所有文件
 *
 * Created by zhangfeng on 2015/1/7.
 */
public class ZipFileRecordReader extends RecordReader<Text, BytesWritable> {
    /** InputStream used to read the ZIP file from the FileSystem */
    private FSDataInputStream fsin;

    /** zip文件格式解析 */
    private ZipInputStream zip;

    /** 未压缩文件名 */
    private Text currentKey;

    /** 未压缩文件内容*/
    private BytesWritable currentValue;

    /** 是否完成进度标志 */
    private boolean isFinished = false;

    /**
     * 从FileSystem初始化和打开zip格式文件
     */
    @Override
    public void initialize( InputSplit inputSplit, TaskAttemptContext taskAttemptContext )
            throws IOException, InterruptedException
    {
        FileSplit split = (FileSplit) inputSplit;
        Configuration conf = taskAttemptContext.getConfiguration();
        Path path = split.getPath();
        FileSystem fs = path.getFileSystem( conf );

        // 打开ZIP文件
        fsin = fs.open( path );
        zip = new ZipInputStream( fsin );
    }

    /**
     * This is where the magic happens, each ZipEntry is decompressed and
     * readied for the Mapper. The contents of each file is held *in memory*
     * in a BytesWritable object.
     *
     * If the ZipFileInputFormat has been set to Lenient (not the default),
     * certain exceptions will be gracefully ignored to prevent a larger job
     * from failing.
     */
    @Override
    public boolean nextKeyValue()
            throws IOException, InterruptedException
    {
        ZipEntry entry = null;
        try
        {
            entry = zip.getNextEntry();
        }
        catch ( ZipException e )
        {
            if ( ZipFileInputFormat.getLenient() == false )
                throw e;
        }

        // 合法性检查
        if ( entry == null )
        {
            isFinished = true;
            return false;
        }

        // 获取文件名称
        currentKey = new Text( entry.getName() );

        // 读取文件内容
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] temp = new byte[8192];
        while ( true )
        {
            int bytesRead = 0;
            try
            {
                bytesRead = zip.read( temp, 0, 8192 );
            }
            catch ( EOFException e )
            {
                if ( ZipFileInputFormat.getLenient() == false )
                    throw e;
                return false;
            }
            if ( bytesRead > 0 )
                bos.write( temp, 0, bytesRead );
            else
                break;
        }
        zip.closeEntry();

        // 未压缩内容
        currentValue = new BytesWritable( bos.toByteArray() );
        return true;
    }

    /**
     * 回去文件读取进度，这只是一个简单实现
     */
    @Override
    public float getProgress()
            throws IOException, InterruptedException
    {
        return isFinished ? 1 : 0;
    }

    /**
     * 返回当前Key (压缩文件名称)
     */
    @Override
    public Text getCurrentKey()
            throws IOException, InterruptedException
    {
        return currentKey;
    }

    /**
     * 返回当前Value (压缩文件内容)
     */
    @Override
    public BytesWritable getCurrentValue()
            throws IOException, InterruptedException
    {
        return currentValue;
    }

    /**
     * 关闭zip文件句柄
     */
    @Override
    public void close()
            throws IOException
    {
        try { zip.close(); } catch ( Exception ignore ) { }
        try { fsin.close(); } catch ( Exception ignore ) { }
    }
}
