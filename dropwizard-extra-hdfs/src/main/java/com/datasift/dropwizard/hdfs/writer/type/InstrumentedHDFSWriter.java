package com.datasift.dropwizard.hdfs.writer.type;


import com.datasift.dropwizard.hdfs.config.HDFSConfiguration;
import com.datasift.dropwizard.hdfs.metrics.HDFSInstrumentation;
import com.datasift.dropwizard.hdfs.writer.AbstractHDFSWriter;
import com.datasift.dropwizard.hdfs.writer.IHDFSWriter;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.TimerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;


import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created with IntelliJ IDEA.
 * User: justinhancock
 * Date: 15/11/2012
 * Time: 16:44
 * To change this template use File | Settings | File Templates.
 */
public class InstrumentedHDFSWriter implements IHDFSWriter {
    private final AbstractHDFSWriter client;


    /**
     * The instrumentation for this {@link AbstractHDFSWriter}.
     */
    private final HDFSInstrumentation metrics;
    /**
     * Builds a new {@link AbstractHDFSWriter} according to the given
     * {@link HDFSConfiguration}.
     * <p>
     * If instrumentation
     * {@link HDFSConfiguration#instrumented is enabled} in the
     * configuration, this will build an {@link InstrumentedHDFSWriter}
     * wrapping the given {@link AbstractHDFSWriter}.
     * <p>
     * If instrumentation is not enabled, the given {@link AbstractHDFSWriter} will be
     * returned verbatim.
     *
     * @param configuration an {@link HDFSConfiguration} defining the
     *                      {@link AbstractHDFSWriter}s parameters
     * @param client        an underlying {@link AbstractHDFSWriter} implementation
     * @return an {@link AbstractHDFSWriter} that satisfies the configuration of
     *         instrumentation
     */
    public static InstrumentedHDFSWriter wrap(final HDFSConfiguration configuration,
                                              final IHDFSWriter client) {
        if (configuration.isInstrumented()) {
            return new InstrumentedHDFSWriter(client);
        } else {
            return (InstrumentedHDFSWriter)client;
        }
    }

    /**
     * Creates a new {@link InstrumentedHDFSWriter} for the given underlying
     * client.
     * <p>
     * The {@link com.yammer.metrics.Metrics#defaultRegistry() default} {@link com.yammer.metrics.core.MetricsRegistry}
     * will be used to register the {@link com.yammer.metrics.core.Metric}s.
     *
     * @param client the underlying {@link IHDFSWriter} implementation to
     *               dispatch requests
     */
    public InstrumentedHDFSWriter(final IHDFSWriter client) {
        this(client, Metrics.defaultRegistry());
    }

    /**
     * Creates a new {@link InstrumentedHDFSWriter} for the given underlying
     * client.
     * <p>
     * Instrumentation will be registered with the given
     * {@link com.yammer.metrics.core.MetricsRegistry}.
     * <p>
     * A new {@link HDFSInstrumentation} container will be created for this
     * {@link IHDFSWriter} with the given {@link com.yammer.metrics.core.MetricsRegistry}.
     *
     * @param client   the underlying {@link AbstractHDFSWriter} implementation to
     *                 dispatch requests
     * @param registry the {@link com.yammer.metrics.core.MetricsRegistry} to register {@link com.yammer.metrics.core.Metric}s
     *                 with
     */
    public InstrumentedHDFSWriter(final IHDFSWriter client,
                                  final MetricsRegistry registry) {
        this(client, new HDFSInstrumentation(client, registry));
    }

    /**
     * Creates a new {@link InstrumentedHDFSWriter} for the given underlying
     * client.
     * <p>
     * Instrumentation will be contained by the given
     * {@link HDFSInstrumentation} instance.
     * <p>
     * <i>Note: this is only really useful for sharing instrumentation between
     * multiple {@link AbstractHDFSWriter} instances, which only really makes sense for
     * instances configured for the same cluster, but with different client-side
     * settings. <b>Use with caution!!</b></i>
     *
     * @param client  the underlying {@link AbstractHDFSWriter} implementation to
     *                dispatch requests
     * @param metrics the {@link HDFSInstrumentation} containing the
     *                {@link com.yammer.metrics.core.Metric}s to use
     */
    public InstrumentedHDFSWriter(final IHDFSWriter client,
                                  final HDFSInstrumentation metrics) {
        this.client = (AbstractHDFSWriter)client;
        this.metrics = metrics;
    }

    @Override
    public void instance(String path, SequenceFile.CompressionType compType, CompressionCodec codec, boolean append, Writable key, Writable value) throws IOException {
        client.instance(path,compType,codec,append,key,value);
    }

    @Override
    public void append(byte[] key, byte[] val) throws IOException {
        client.append(key,val);
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean exists(String path) throws IOException {
        return client.exists(path);  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long getRecordsWrittenCounter() {
        return client.getRecordsWrittenCounter();  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long getSynchsCounter() {
        return client.getSynchsCounter();  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long getCloseCounter() {

        return client.getCloseCounter();  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long getBlocksWritten() {

        return client.getBlocksWritten();  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Path getPath() {
        return client.getPath();  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void incrementBytesWritten(byte[] bytes) {
        client.incrementBytesWritten(bytes);
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void incrementBytesWritten(long bytes) {
        client.incrementBytesWritten(bytes);
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long getBytesWritten() {
        //final TimerContext ctx = metrics.getCreates().time();
        AtomicLong val = new AtomicLong();


        return client.getBytesWritten();  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void incrementWriterCounter() {
        client.incrementWriterCounter();
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void incrementHDFSSynchsCounter() {
        client.incrementHDFSSynchsCounter();

    }

    @Override
    public void incrementCloseCounter() {
        client.incrementCloseCounter();
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void incrementBlocksWritten() {

        client.incrementBlocksWritten();

    }

    @Override
    public FileSystem getFileSystem() throws IOException {
        return client.getFileSystem();

    }

    @Override
    public Configuration getHDFSConfiguration() {
        return client.getHDFSConfiguration();
    }

    @Override
    public void sync() throws IOException {
        client.sync();
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void close() throws IOException {
        client.close();
        //To change body of implemented methods use File | Settings | File Templates.
    }




}
