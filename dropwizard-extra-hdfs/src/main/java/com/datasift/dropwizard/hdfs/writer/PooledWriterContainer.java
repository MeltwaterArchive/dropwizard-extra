package com.datasift.dropwizard.hdfs.writer;

/**
 * Created with IntelliJ IDEA.
 * User: justinhancock
 * Date: 31/10/2012
 * Time: 11:42
 * To change this template use File | Settings | File Templates.
 */
public class PooledWriterContainer {
    private AbstractHDFSWriter writer = null;
    private String path = null;
    private long lastUpdateTime= 0;
    private Boolean inuse = false;
    public PooledWriterContainer(AbstractHDFSWriter writer, String path) {
        this.writer = writer;
        this.path = path;
        this.lastUpdateTime = System.currentTimeMillis();


     }

    public AbstractHDFSWriter getWriter(){
        synchronized (this){
        this.lastUpdateTime = System.currentTimeMillis();
        return writer;
        }

    }
    public boolean status(){

        return inuse;
    }

    @Override
    public int hashCode(){
        return path.hashCode();

    }
    public long getLastAccessTime() {
        synchronized (this){
            return this.lastUpdateTime;
        }


    }
}
