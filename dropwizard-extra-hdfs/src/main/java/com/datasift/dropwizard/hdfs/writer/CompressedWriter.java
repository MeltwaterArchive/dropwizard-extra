package com.datasift.dropwizard.hdfs.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: justinhancock
 * Date: 31/10/2012
 * Time: 09:57
 * To change this template use File | Settings | File Templates.
 */
public class CompressedWriter extends AbstractHDFSWriter{

    private boolean isFinished = false;
    private FSDataOutputStream fsOut = null;
    private FileSystem hdfs= null;
    private CompressionOutputStream cmpOut;
    public CompressedWriter(final String path, final SequenceFile.CompressionType compType, final CompressionCodec codec, final boolean append, final Writable key, final Writable value){
        super(path, compType,codec,append, key,value);

    }

    @Override
    public void instance(final String path, final SequenceFile.CompressionType compType, final CompressionCodec codec, final boolean append, final Writable key, final Writable value) throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
        Configuration conf = new Configuration();
        Path dstPath = new Path(path);
        this.hdfs = dstPath.getFileSystem(conf);

        if (conf.getBoolean("hdfs.append.support", false) == true && append && hdfs.isFile
                (dstPath)) {
            fsOut = hdfs.append(dstPath);
        } else {
            fsOut = hdfs.create(dstPath);
        }
        cmpOut = codec.createOutputStream(fsOut);
        isFinished = false;
    }




    @Override
    public void append(byte[] key, byte[] val) throws IOException {
        if (isFinished) {
            cmpOut.resetState();
            isFinished = false;
        }
        cmpOut.write(val);
        this.incrementWriterCounter();
        this.incrementBytesWritten(val);

    }

    @Override
    public void sync() throws IOException {
        if (!isFinished) {
            cmpOut.finish();
            isFinished = true;
        }
        fsOut.flush();
        fsOut.sync();
        this.incrementHDFSSynchsCounter();

    }

    @Override
    public void close() throws IOException {

        sync();

        cmpOut.close();
        incrementCloseCounter();
    }






}
