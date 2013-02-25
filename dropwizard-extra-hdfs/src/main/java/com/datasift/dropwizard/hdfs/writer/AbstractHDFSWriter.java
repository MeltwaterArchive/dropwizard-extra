package com.datasift.dropwizard.hdfs.writer;

/**
 * Created with IntelliJ IDEA.
 * User: justinhancock
 * Date: 30/10/2012
 * Time: 12:06
 * To change this template use File | Settings | File Templates.
 */
import com.datasift.dropwizard.hdfs.metrics.HDFSInstrumentation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractHDFSWriter implements IHDFSWriter {

    private AtomicLong hdfsSynchs = new AtomicLong(0);
    private AtomicLong recordsWritten = new AtomicLong(0);
    private AtomicInteger blocksWritten = new AtomicInteger(0);
    private AtomicLong closeCounter = new AtomicLong(0);
    private AtomicLong bytesWritten = new AtomicLong(0);
    private FileSystem hdfs =null;
    private Configuration configuration = null;
    private String path = null;
    private Path dstPath = null;


    public AbstractHDFSWriter (String path, CompressionType compType, CompressionCodec codec,final boolean append, Writable key, Writable value, HDFSInstrumentation instrumentation){
        try{
            this.instance(path,compType,codec, append, key, value);
            this.setPath(path);
        }catch (IOException e){
            e.printStackTrace();
        }

    }
    public AbstractHDFSWriter (String path, CompressionType compType, CompressionCodec codec,final boolean append, Writable key, Writable value){
        try{
            this.instance(path,compType,codec, append, key, value);
            this.setPath(path);
        }catch (IOException e){
            e.printStackTrace();
        }

    }
    protected AbstractHDFSWriter(){

    }

    public void setPath(String path){
        this.path = path;
        dstPath = new Path(path);
    }

    public Path getPath(){
        return dstPath;

    }
    public int hashCode(){
        return path.hashCode();
    }
    public void incrementWriterCounter(int val){
        recordsWritten.addAndGet(val);

    }
    public void incrementWriterCounter(){
        recordsWritten.getAndIncrement();
    }
   public void incrementCloseCounter(){
       closeCounter.incrementAndGet();

   }

    public long getCloseCounter(){
        return closeCounter.get();
    }
    public void incrementHDFSSynchsCounter(){
        //this.getFileSystem().getFileStatus(getPath())
        //getPath().
        hdfsSynchs.incrementAndGet();
    }

    public long getRecordsWrittenCounter(){
        return recordsWritten.get();
    }

    public long getSynchsCounter(){
        return hdfsSynchs.get();
    }
    public void incrementBlocksWritten(){
        blocksWritten.incrementAndGet();
    }
    public long getBlocksWritten(){
        long len=-1;
         try {
            FileStatus fs = getFileSystem().getFileStatus(getPath()) ;
            len = getFileSystem().getFileBlockLocations(fs,0,fs.getLen()).length;
         }catch (IOException e){
                   e.printStackTrace();
         }
      return len;
    }
    public void incrementBytesWritten(byte[] bytes){
        this.incrementBytesWritten(bytes.length);
    }
    public void incrementBytesWritten(long bytes){
        bytesWritten.addAndGet(bytes);
    }


   public long getBytesWritten(){
       return bytesWritten.get();
   }
   public FileSystem getFileSystem() throws IOException{
       if(hdfs == null){
           hdfs = getPath().getFileSystem(configuration);
       }
       return hdfs;
   }
    public Configuration getHDFSConfiguration(){
        if(configuration == null){
            configuration = new Configuration();
         }
        return configuration;
    }

    public boolean exists(String path) throws IOException{
        return getFileSystem().exists(new Path(path));

    }


}
