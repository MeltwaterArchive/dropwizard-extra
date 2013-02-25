package com.datasift.dropwizard.hdfs.writer;

/**
 * Created with IntelliJ IDEA.
 * User: justinhancock
 * Date: 26/10/2012
 * Time: 09:48
 * To change this template use File | Settings | File Templates.
 */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import java.io.IOException;


public interface IHDFSWriter {

    public void instance (final String path, final CompressionType compType, final CompressionCodec codec, final boolean append, final Writable key, final Writable value) throws IOException;
//    public void open(String filePath) throws IOException;
//
//    // public void open(String filePath, CompressionCodec codec, CompressionType
//    // cType) throws IOException;
//
//    public void open(String filePath, CompressionCodec codec,
//                     CompressionType cType) throws IOException;

    // public void append(long key, byte [] val) throws IOException;

    /**
     * Appends a byte array to the implemented output type, all data be it strings and so will need to be converted to byte arrays.
     * @param key
     * @param val
     * @throws IOException
     */
    public void append(byte[] key, byte[] val) throws IOException;

    /**
     * This allows implementation of a synchronisation method, this used by HDFS for closing blocks or writing the end of comprssed stream
     * @throws IOException
     */
    public void sync() throws IOException;

    /**
     * Close method is needed to finalize a HDFS Block so that it is written
     * @throws IOException
     */

    public void close() throws IOException;


    /**
     * Check for the existence of the file on HDFS before writing data.
     * @param path
     * @return
     * @throws IOException
     */
    public boolean exists(String path) throws IOException;

    /**
     * Return numnber of records written by the append method, useful for metrics.-
     * @return
     */
    public long getRecordsWrittenCounter();
    public long getSynchsCounter();
    public long getCloseCounter();
    public long getBlocksWritten();
    public Path getPath();
    void incrementBytesWritten(byte[] bytes);
    void incrementBytesWritten(long bytes);


    public long getBytesWritten();


    public void incrementWriterCounter();
    public void incrementHDFSSynchsCounter();
    public void incrementCloseCounter();
    public void incrementBlocksWritten();
    public FileSystem getFileSystem() throws IOException;
    public Configuration getHDFSConfiguration();

}



