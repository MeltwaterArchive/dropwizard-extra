package com.datasift.dropwizard.hdfs.reader;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: justinhancock
 * Date: 05/11/2012
 * Time: 12:15
 * To change this template use File | Settings | File Templates.
 */
public interface IHDFSReader {
    public void instance (final String path, final SequenceFile.CompressionType compType, final CompressionCodec codec) throws IOException;



    public byte[] read() throws IOException;
    public byte[] read(String offset) throws IOException;
    public byte[] read(byte[] offset) throws IOException;


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
    public long recordsRead();
    public String getPath();
    public long getHDFSSynchs();

}
