package com.datasift.dropwizard.hdfs.writer;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: justinhancock
 * Date: 30/10/2012
 * Time: 12:32
 * To change this template use File | Settings | File Templates.
 */
public class SequenceFileWriter  extends AbstractHDFSWriter{
    private Path outpath = null;
    private Configuration conf = null;
    private FileSystem fs = null;
    private SequenceFile.Writer writer = null;



    public SequenceFileWriter(String path, SequenceFile.CompressionType compType, CompressionCodec codec, final boolean append, Writable key, Writable value){
        super(path, compType,codec,append, key,value);

    }


    @Override
    public void instance(final String path, final SequenceFile.CompressionType compType, final CompressionCodec codec, final boolean append, Writable key, Writable value) throws IOException {


        fs = getFileSystem();
        if (conf.getBoolean("hdfs.append.support", false) == false && append){
            throw new IOException("HDFS append support is not enabled.")   ;

        }
        else if (conf.getBoolean("hdfs.append.support", false) == true && append && fs.isFile
                (outpath)) {
            FSDataOutputStream outStream = fs.append(outpath);
            this.writer = SequenceFile.createWriter(conf, outStream, key.getClass(),
                    value.getClass(), compType, codec);
        } else {
            if(!fs.exists(outpath)){
                this.writer = SequenceFile.createWriter(fs,conf,outpath, key.getClass() ,value.getClass(),compType,codec);
            }else{
                throw new IOException("Output path already exists: " + path);
            }

        }



    }



    @Override
    public void append(byte[] key, byte[] val) throws IOException {
        writer.append(key, val);
       this.incrementWriterCounter();
        this.incrementBytesWritten(val);


    }
    public void append(Writable key, Writable val) throws IOException {
        writer.append(key, val);
        //This is bit kludgey and not the best way to get the bytes written, it will be padded out with UTF bytes as its a string representation.
        this.incrementBytesWritten(val.toString().getBytes());
       this.incrementWriterCounter();

    }

    @Override
    public void sync() throws IOException {
        writer.sync();
        writer.syncFs();
        this.incrementHDFSSynchsCounter();
    }

    @Override
    public void close() throws IOException {

         this.sync();
        writer.close();
        incrementCloseCounter();


    }






}
