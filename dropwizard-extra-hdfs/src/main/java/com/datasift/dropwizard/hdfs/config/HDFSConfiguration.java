package com.datasift.dropwizard.hdfs.config;

/**
 * Created with IntelliJ IDEA.
 * User: justinhancock
 * Date: 25/10/2012
 * Time: 11:47
 * To change this template use File | Settings | File Templates.
 */
import com.yammer.dropwizard.util.Duration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.codehaus.jackson.annotate.JsonProperty;
//import org.codehaus.jackson.annotate.JsonProperty;
import javax.validation.constraints.NotNull;


//import org.apache.hadoop.mapred.FileOutputFormat;

import java.lang.String;

public class HDFSConfiguration  {

    protected Config config = new Config()   ;



    @JsonProperty
    @NotNull
    protected Duration flushInterval = Duration.seconds(1);


    /**
     * Whether the {@link com.datasift.dropwizard.hdfs.writer.IHDFSWriter} should be instrumented with
     * {@link com.yammer.metrics.core.Metric}s.
     */
    @JsonProperty
    protected boolean instrumented = true;


    @JsonProperty
    @NotNull
    protected String baseHDFSURI =  "hdfs://localhost";

    @JsonProperty
    @NotNull
    protected String outputDirectory = "/output";

    /**
     *
     */
    @JsonProperty
    @NotNull
    protected Duration connectionTimeout = Duration.seconds(5);


    /**
     * The output format that should be used to write the data for the Hadoop writer
     */
    @JsonProperty
    @NotNull
    protected String outputFormat = "TextOutputFormat";

    /**
     * Whether the data should be compressed to HDFS
     */
    @JsonProperty
    protected Boolean compressOutput = true;

    /**
     * Whether to enable or disable append support for HDFS
     */
    @JsonProperty
    protected Boolean append = true;

    public Boolean getAppend() {
        return append;
    }

    /**
     *
     */
    @JsonProperty
    protected String compressionCodec = "Default";

    @JsonProperty
    protected String compressionType = "BLOCK";



    @JsonProperty
    protected String writerKey = "BytesWritable";
   // @JsonProperty
    protected String writerValue = "Text";

    public String getWriterValue() {
        return writerValue;
    }

    public String getWriterKey() {
        return writerKey;
    }

    @JsonProperty
   // protected Config config.GenericObjectPool.
    protected int maxactive = GenericObjectPool.DEFAULT_MAX_ACTIVE ;//config.maxActive


    @JsonProperty
    protected int maxidle = GenericObjectPool.DEFAULT_MAX_IDLE;

    @JsonProperty
    protected long maxwait = GenericObjectPool.DEFAULT_MAX_WAIT;

    @JsonProperty
    protected long minidletimebeforeeviction = GenericObjectPool.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS;


    @JsonProperty
    protected int minidle =GenericObjectPool.DEFAULT_MIN_IDLE;




    public SequenceFile.CompressionType getCompressionType(){
        SequenceFile.CompressionType type= SequenceFile.CompressionType.BLOCK;
        if(compressionType.equalsIgnoreCase("BLOCK")) {
            type = SequenceFile.CompressionType.BLOCK;
        }else if(compressionType.equalsIgnoreCase("RECORD")){
            type=SequenceFile.CompressionType.RECORD;

        }else if(compressionType.equalsIgnoreCase("NONE")){
            type=SequenceFile.CompressionType.NONE;
        }




        return type;
    }
    public int getMaxactive() {
        return maxactive;
    }

    public int getMaxidle() {
        return maxidle;
    }

    public long getMaxwait() {
        return maxwait;
    }

    public long getMinidletimebeforeeviction() {
        return minidletimebeforeeviction;
    }

    public int getMinidle() {
        return minidle;
    }

    /**
     * @see
     */
    public Duration getFlushInterval() {
        return flushInterval;
    }




    /**
     * @see
     */
    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    /**
     * @see
     * */
    public boolean isInstrumented() {
        return instrumented;
    }
    public String getBaseHDFSURI() {
        return baseHDFSURI;
    }

    public String getOutputDirectory() {
        return outputDirectory;
    }

    public String getOutputFormat() {
        return outputFormat;
    }

    public Boolean getCompressOutput() {
        return compressOutput;
    }

    public String getCompressionCodec() {
        return compressionCodec;
    }


}
