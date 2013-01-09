package com.datasift.dropwizard;

import com.datasift.dropwizard.config.ZooKeeperConfiguration;
import com.datasift.dropwizard.health.ZooKeeperQuorumHealthCheck;
import com.yammer.dropwizard.config.Environment;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * Factory to generate {@link ManagedZooKeeperClient} instances from the config.
 */
public class ZooKeeperClientFactory {

    private final Environment environment;

    /**
     * Constructor
     * @param environment See {@link Environment}. This is the Dropwizard Environment.
     */
    ZooKeeperClientFactory(Environment environment) {
        this.environment = environment;
    }


    /**
     * Function to build the {@link ZooKeeper} class
     * It also adds {@link ZooKeeperQuorumHealthCheck} to the list of Dropwizard Healthchecks
     * @param config ZooKeeper Configuration {@link ZooKeeperConfiguration}
     * @param watcher Watcher class to watch for Zookeeper events. See {@link Watcher}
     * @return {@link ManagedZooKeeperClient} instance
     * @throws IOException
     * @throws InterruptedException
     */
    public ZooKeeper build(ZooKeeperConfiguration config, Watcher watcher) throws IOException, InterruptedException {
        return build(config, watcher, 0, null);
    }


    /**
     * Function to build the {@link ZooKeeper} class with existing sessionId and sessionPwd
     * It also adds {@link ZooKeeperQuorumHealthCheck} to the list of Dropwizard Healthchecks
     * @param config ZooKeeper Configuration {@link ZooKeeperConfiguration}
     * @param watcher Watcher class to watch for Zookeeper events. See {@link Watcher}
     * @param sessionId Session Id with ZooKeeper
     * @param sessionPasswd Session Password with ZooKeeper
     * @return {@link ManagedZooKeeperClient} instance
     * @throws IOException
     * @throws InterruptedException
     */
    public ZooKeeper build(ZooKeeperConfiguration config, Watcher watcher,
                           long sessionId, byte[] sessionPasswd) throws IOException, InterruptedException {
        // Build connection string
        StringBuilder connectionString = new StringBuilder();
        int port = config.getPort();
        for(String host: config.getHosts()) {
            connectionString.append(host);
            connectionString.append(port + ",");
        }
        String cString = connectionString.substring(0, connectionString.length() - 1);

        ZooKeeper zk = null;
        if (sessionId == 0 && sessionPasswd == null) {
            zk = new ZooKeeper(cString, (int) config.getSessionTimeout().toSeconds(), watcher);
        } else {
            zk = new ZooKeeper(cString, (int) config.getSessionTimeout().toSeconds(), watcher,
                                                                        sessionId, sessionPasswd);
        }

        ManagedZooKeeperClient managedZooKeeperClient = new ManagedZooKeeperClient(zk);
        environment.manage(managedZooKeeperClient);
        environment.addHealthCheck(ZooKeeperQuorumHealthCheck.forNodes(config.getHosts(),
                config.getPort(), "zookeeper-healthcheck"));

        return zk;

    }

}
