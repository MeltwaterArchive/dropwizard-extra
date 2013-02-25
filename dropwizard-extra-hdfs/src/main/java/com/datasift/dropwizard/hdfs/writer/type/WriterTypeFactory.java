package com.datasift.dropwizard.hdfs.writer.type;

import org.apache.hadoop.io.Writable;

/**
 * Created with IntelliJ IDEA.
 * User: justinhancock
 * Date: 02/11/2012
 * Time: 10:10
 * To change this template use File | Settings | File Templates.
 */


public class WriterTypeFactory {
    private static final String WRITABLE_PKG="org.apache.hadoop.io.";
    public static Writable createWritable(String name) throws ClassNotFoundException, InstantiationException,IllegalAccessException{

        /* @TODO: Tidy this up, its just plain dangerous as is.

         */
        Writable writer;

        Class cls ;
        cls =   Class.forName(WRITABLE_PKG + name);



        writer = (Writable)cls.newInstance();
        return writer;

    }
}
