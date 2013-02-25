package com.datasift.dropwizard.hdfs.writer.pool;

import com.datasift.dropwizard.hdfs.config.HDFSConfiguration;
import com.datasift.dropwizard.hdfs.writer.*;
import org.apache.commons.pool.KeyedPoolableObjectFactory;

import java.io.IOException;


/**
 * Created with IntelliJ IDEA.
 * User: justinhancock
 * Date: 01/11/2012
 * Time: 12:55
 * To change this template use File | Settings | File Templates.
 */
public class HDFSWriterFactory<K , V extends IHDFSWriter> implements KeyedPoolableObjectFactory<K, V> {
    private long recordsWritten;
    private long synchronisations;
    private long writersOpened;

    HDFSConfiguration configuration = null;
    public  enum WRITERS  {
        SEQUENCEWRITER,
        TEXTWRITER,
        COMPRESSEDWRITER

    };


    public HDFSWriterFactory(HDFSConfiguration configuration){
        this.configuration = configuration;
    }

    //@Override
    @Override
   public V makeObject(K key)  throws Exception{




        return (V)WriterFactory.createWriter(key.toString(),configuration);


    }

    @Override
    public void destroyObject(K key, V resource) throws Exception {
        if (resource != null){

            resource.close();
        }

    }

    @Override
    public boolean validateObject(K key, V resource) {
        boolean status = false;
        if (resource !=null){
          //IHDFSWriter writer=   resource;
            try{
                status=resource.exists(key.toString())  ;
            }catch (IOException e){
                e.printStackTrace();


            }
        }

        return status;
    }

    @Override
    public void activateObject(K key, V resource) throws Exception {

    }

    @Override
    public void passivateObject(K key, V resource) throws Exception {
        if (resource != null){
            AbstractHDFSWriter var =  (AbstractHDFSWriter)resource;
            var.sync();
        }

    }



}
