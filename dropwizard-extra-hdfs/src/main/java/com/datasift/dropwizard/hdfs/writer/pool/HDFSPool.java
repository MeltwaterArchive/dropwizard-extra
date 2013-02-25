package com.datasift.dropwizard.hdfs.writer.pool;

/**
 * Created with IntelliJ IDEA.
 * User: justinhancock
 * Date: 01/11/2012
 * Time: 12:49
 * To change this template use File | Settings | File Templates.
 */
import com.datasift.dropwizard.hdfs.config.HDFSConfiguration;
//import org.apache.tomcat.dbcp.pool.BaseKeyedPoolableObjectFactory;
//import org.apache.tomcat.dbcp.pool.KeyedPoolableObjectFactory;
//import org.apache.tomcat.dbcp.pool.impl.GenericKeyedObjectPool;
import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;


import java.lang.Override;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


public class HDFSPool<K,V> extends GenericKeyedObjectPool<K,V >{

    private Config poolConfig = null;
    private HDFSConfiguration hdfsConfiguration = null;
    private ConcurrentHashMap<K,V> objectPool =null;
    private ConcurrentHashMap<K, AtomicLong> writes;
    private AtomicLong totalWrites;






    public HDFSPool(HDFSConfiguration hadoopConf, KeyedPoolableObjectFactory<K,V> factory){
        super(factory);
        hadoopConf.getBaseHDFSURI();
        poolConfig = new Config();
        poolConfig.maxActive = hadoopConf.getMaxactive();
        poolConfig.maxIdle = hadoopConf.getMaxidle();
        poolConfig.maxWait = hadoopConf.getMaxwait();
        poolConfig.minEvictableIdleTimeMillis =hadoopConf.getMinidletimebeforeeviction();
        poolConfig.minIdle = hadoopConf.getMinidle();
        objectPool = new ConcurrentHashMap<K, V>(6);
        writes = new ConcurrentHashMap<K, AtomicLong>(6);
        totalWrites = new AtomicLong(0);


    }
//    public HDFSPool<K,V>(HadoopConfiguration hadoopConf, final KeyedPoolableObjectFactory<K,V> factory){
//       // super(factory);
//        hadoopConf.getBaseHDFSURI();
//        poolConfig = new Config();
//        poolConfig.maxActive = hadoopConf.getMaxactive();
//        poolConfig.maxIdle = hadoopConf.getMaxidle();
//        poolConfig.maxWait = hadoopConf.getMaxwait();
//        poolConfig.minEvictableIdleTimeMillis =hadoopConf.getMinidletimebeforeeviction();
//        poolConfig.minIdle = hadoopConf.getMinidle();
//        objectPool = new ConcurrentHashMap<K, V>(6);
//
//        //super(factory,poolConfig);
//          super.setConfig(poolConfig);
//
//
//
//    }


    @Override
    public void  returnObject(K key, V object) throws Exception{
        super.returnObject(key, object);

    }


    @Override
    public V borrowObject(K key) throws Exception{




        V poolItem = super.borrowObject(key);
        objectPool.put(key, poolItem);
       // super.returnObject();
        return poolItem;
    }

//
//    void test(){
//
//       // new GenericKeyedObjectPoolFactory()
//        HDFSWriterFactory factory = new HDFSWriterFactory(hadoopConfiguration);
//
//
//        GenericKeyedObjectPool pool = new GenericKeyedObjectPool(factory,poolConfig);
//        try {
//            pool.addObject("");
//        } catch (Exception e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }
//    }
}
