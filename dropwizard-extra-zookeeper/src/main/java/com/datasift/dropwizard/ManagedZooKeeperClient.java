package com.datasift.dropwizard;

import com.yammer.dropwizard.lifecycle.Managed;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * A managed ZooKeeper client. See {@link Managed}
 * It ensures that the ZooKeeper connection is closed when the application stops
 */
public class ManagedZooKeeperClient implements Managed {

    private ZooKeeper zooKeeper = null;

    /**
     * Constructor. Accepts a {@link ZooKeeper} instance
     * @param zk {@link ZooKeeper} instance
     * @throws IOException
     */
    ManagedZooKeeperClient(ZooKeeper zk) throws IOException {
        zooKeeper = zk;
    }


    /**
     * Does nothing in this case as the constructor initialization of {@link ZooKeeper} class
     * establishes a connection. The {@link org.apache.zookeeper.Watcher} should check
     * for connection establishment
     * @throws Exception
     */
    @Override
    public void start() throws Exception {
        // Its the watchers responsibility to make sure the connections has been established
    }

    /**
     * Ensures that the {@link ZooKeeper} connection is closed when the application stops
     * @throws Exception
     */
    @Override
    public void stop() throws Exception {
        zooKeeper.close();
    }

}
