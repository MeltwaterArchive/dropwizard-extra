package com.datasift.dropwizard;

import com.yammer.dropwizard.lifecycle.Managed;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import java.io.IOException;

/**
 * A managed ZooKeeper client. See {@link Managed}
 * It ensures that the ZooKeeper connection is closed when the application stops
 */
public class ZooKeeperClient extends ZooKeeper implements Managed {

    /**
     * See {@link ZooKeeper#ZooKeeper(String, int, org.apache.zookeeper.Watcher)}
     * @param connectionString
     * @param sessionTimeout
     * @param watcher
     * @throws IOException
     */
    ZooKeeperClient(String connectionString, int sessionTimeout, Watcher watcher) throws IOException {
        super(connectionString, sessionTimeout, watcher);
    }

    /**
     * See {@link ZooKeeper#ZooKeeper(String, int, org.apache.zookeeper.Watcher, long, byte[])}
     * @param connectionString
     * @param sessionTimeout
     * @param watcher
     * @param sessionId
     * @param sessionPasswd
     * @throws IOException
     */
    ZooKeeperClient(String connectionString, int sessionTimeout, Watcher watcher,
                                            long sessionId, byte[] sessionPasswd) throws IOException {
        super(connectionString, sessionTimeout, watcher, sessionId, sessionPasswd);
    }

    /**
     * Does nothing in this case as the constructor initialization of {@link ZooKeeper} class
     * establishes a connection
     * @throws Exception
     */
    @Override
    public void start() throws Exception {
        // Its the watchers responsibility to make sure the connections has been established
    }

    /**
     * Ensures that the {@link ZooKeeper} connection is closed with the application stops
     * @throws Exception
     */
    @Override
    public void stop() throws Exception {
        super.close();
    }

}
