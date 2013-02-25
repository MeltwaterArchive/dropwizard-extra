package com.datasift.dropwizard.hdfs.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: justinhancock
 * Date: 31/10/2012
 * Time: 08:46
 * To change this template use File | Settings | File Templates.
 */
public class TextWriter extends AbstractHDFSWriter{
    private FSDataOutputStream outStream;
    //private FileSystem hdfs;
    private Path dstPath;



    public TextWriter(String path, SequenceFile.CompressionType compType, CompressionCodec codec, final boolean append, Writable key, Writable value){
        super(path, compType,codec,append, key,value);

    }

    @Override
    public void instance(String path, SequenceFile.CompressionType compType, CompressionCodec codec, final boolean append, Writable key, Writable value) throws IOException {
        Configuration conf = new Configuration();
        this.dstPath = new Path(path);

        FileSystem hdfs = this.getFileSystem();

        boolean appending = append;
        if (conf.getBoolean("hdfs.append.support", false) == true && append && hdfs.isFile
                (dstPath)) {
            outStream = hdfs.append(dstPath);
            appending = true;
        } else {
            if(! hdfs.exists(dstPath)){
                outStream = hdfs.create(dstPath);
            }
            else{
                throw new IOException("Output path already exists: " + path);
            }
        }





    }

    @Override
    public void append(byte[] key, byte[] val) throws IOException {
        outStream.write(val);
        this.incrementWriterCounter();
        this.incrementBytesWritten(val);
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void sync() throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
        outStream.flush();
        outStream.sync();
        this.incrementHDFSSynchsCounter();
    }

    @Override
    public void close() throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
        outStream.flush();
        outStream.sync();
        outStream.close();
        incrementCloseCounter();
        FileStatus fileStatus =getFileSystem().getFileStatus(dstPath);

    }






}
