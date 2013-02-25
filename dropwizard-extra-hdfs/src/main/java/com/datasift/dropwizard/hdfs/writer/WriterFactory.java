package com.datasift.dropwizard.hdfs.writer;

import com.datasift.dropwizard.hdfs.config.HDFSConfiguration;

import com.datasift.dropwizard.hdfs.writer.type.InstrumentedHDFSWriter;
import com.datasift.dropwizard.hdfs.writer.type.WriterTypeFactory;
//import org.apache.hadoop.hdfs.tools.HDFSConcat;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;



/**
 * Created with IntelliJ IDEA.
 * User: justinhancock
 * Date: 31/10/2012
 * Time: 15:20
 * To change this template use File | Settings | File Templates.
 */
public class WriterFactory  {


    private HDFSConfiguration hdfsConfiguration = null;
    public WriterFactory(HDFSConfiguration configuration){
        this.hdfsConfiguration = configuration;

    }


    private static String SEQUENCEWRITER = "SequenceWriter";
    private static String COMPRESSEDWRITER="CompressedWriter";
    private static String TEXTWRITER="TextWriter";

    private static String[] WRITERS = {SEQUENCEWRITER,COMPRESSEDWRITER,TEXTWRITER}       ;

    public  enum Writers  {
        SEQUENCEWRITER,
        TEXTWRITER,
        COMPRESSEDWRITER

    };
    public static void main(String[] args0){
        //createWriter("SequenceWriter","",SequenceFile.CompressionType.BLOCK,new SnappyCodec(),true, new Text(),new Text())  ;

    }


    public static IHDFSWriter createWriter(final String path,final HDFSConfiguration hdfsConfiguration) throws Exception{
        Writable key = WriterTypeFactory.createWritable(hdfsConfiguration.getWriterKey());
        Writable value =WriterTypeFactory.createWritable(hdfsConfiguration.getWriterValue());
        boolean append = hdfsConfiguration.getAppend();
        String type = hdfsConfiguration.getCompressionCodec();
        CompressionCodec codec = com.datasift.dropwizard.hdfs.writer.CodecFactory.getCodec(hdfsConfiguration.getCompressionCodec());
        SequenceFile.CompressionType compType = hdfsConfiguration.getCompressionType();
        IHDFSWriter writer = null;


        if(type.equalsIgnoreCase(SEQUENCEWRITER))   {
            writer= new SequenceFileWriter(path, compType, codec, append, key, value);

        }   else if (type.equalsIgnoreCase(TEXTWRITER)){
            writer =new TextWriter(path, compType, codec, append, key, value);

        } else if (type.equalsIgnoreCase(COMPRESSEDWRITER)){
            writer = new CompressedWriter(path, compType, codec, append, key, value);
        }         else{
            throw new Error("Unknown writer type, check your configuration to ensure you have the correct type, valid types are: " + WRITERS.toString());
        }


       return InstrumentedHDFSWriter.wrap(hdfsConfiguration,writer);








    }
}

