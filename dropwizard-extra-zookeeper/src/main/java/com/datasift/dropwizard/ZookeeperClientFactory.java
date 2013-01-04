package com.datasift.dropwizard;

import com.datasift.dropwizard.config.ZooKeeperConfiguration;
import com.datasift.dropwizard.health.ZooKeeperQuorumHealthCheck;
import com.yammer.dropwizard.config.Environment;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * Factory to generate {@link ZooKeeperClient} instances from the config.
 */
public class ZookeeperClientFactory {

    private final Environment environment;

    /**
     * Constructor
     * @param environment See {@link Environment}. This is the Dropwizard Environment.
     */
    ZookeeperClientFactory(Environment environment) {
        this.environment = environment;
    }


    /**
     * Function to build the {@link ZooKeeperClient} class
     * It also adds {@link ZooKeeperQuorumHealthCheck} to the list of Dropwizard Healthchecks
     * @param config ZooKeeper Configuration {@link ZooKeeperConfiguration}
     * @param watcher Watcher class to watch for Zookeeper events. See {@link Watcher}
     * @return {@link ZooKeeperClient} instance
     * @throws IOException
     * @throws InterruptedException
     */
    public ZooKeeper build(ZooKeeperConfiguration config, Watcher watcher) throws IOException, InterruptedException {
        // Build connection string
        StringBuilder connectionString = new StringBuilder();
        int port = config.getPort();
        for(String host: config.getHosts()) {
            connectionString.append(host);
            connectionString.append(port + ",");
        }
        String cString = connectionString.substring(0, connectionString.length() - 1);

        ZooKeeperClient zk = new ZooKeeperClient(cString,
                                        (int) config.getSessionTimeout().toSeconds(), watcher);
        environment.manage(zk);

        environment.addHealthCheck(ZooKeeperQuorumHealthCheck.forNodes(config.getHosts(),
                                                                       config.getPort(), "zookeeper-healthcheck"));


        return zk;
    }

}
