package com.datasift.dropwizard.hdfs.metrics;

import com.datasift.dropwizard.hdfs.writer.IHDFSWriter;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;

/**
 *
 *
 */
public class HDFSInstrumentation {

    // request timers
    private final Timer creates;
    private final Timer deletes;
    private final Timer flushes;
    private final Timer gets;
    private final Timer writes;
    private final Timer directoriescreated;
    private final Timer blockswritten;

    private final Timer closes;

    //final HBaseClient client,
    public HDFSInstrumentation(final IHDFSWriter client,
                               final MetricsRegistry registry) {

        final Class<? extends IHDFSWriter> clazz =  client.getClass();

        // timers
        creates        = registry.newTimer(clazz, "create",        "writes");
        deletes        = registry.newTimer(clazz, "delete",        "writes");
        flushes        = registry.newTimer(clazz, "flush",         "writes");
        directoriescreated= registry.newTimer(clazz, "directoriescreated", "writes");
        gets           = registry.newTimer(clazz, "get",           "requests");
        writes         = registry.newTimer(clazz, "writes",           "writes");
        blockswritten = registry.newTimer(clazz, "blockswritten", "writes");
        closes         = registry.newTimer(clazz, "closes",        "writes");



            registry.newGauge(clazz, "flushes", "totals", new Gauge<Long>() {
                @Override public Long value() {
                    return client.getSynchsCounter();
                }
            });
            registry.newGauge(clazz, "writes", "totals", new Gauge<Long>() {
                @Override public Long value() {
                    return client.getRecordsWrittenCounter();
                }
            });
            registry.newGauge(clazz, "closes", "totals", new Gauge<Long>() {
                @Override public Long value() {
                    return client.getCloseCounter();
                }
            });
            registry.newGauge(clazz, "blockswritten", "totals", new Gauge<Long>() {
                @Override public Long value() {
                    return client.getBlocksWritten();
                }
            });



    }




        // increment buffer stats


    public Timer getCreates() {
        return creates;
    }




//    public Timer getDeletes() {
//        return deletes;
//    }


    public Timer getFlushes() {
        return flushes;
    }

    public Timer getGets() {
        return gets;
    }


    public Timer getCloses() {
        return closes;
    }
}
