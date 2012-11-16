package com.datasift.dropwizard.zookeeper.util;

import org.apache.zookeeper.common.PathUtils;

/**
 * Simple utility for parsing/validating a ZNode path in ZooKeeper.
 * <p/>
 * Validates ZNode against the rules specified in the
 * <a href="http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#ch_zkDataModel">
 * ZooKeeper Data Model</a>.
 */
public class ZNode {

    private final String path;

    /**
     * Create a {@link ZNode} from the given {@link String}.
     *
     * @see PathUtils#validatePath(String);
     * @param path the path of the ZNode as a {@link String}.
     * @throws IllegalArgumentException if the path is invalid for a ZNode.
     */
    public ZNode(final String path) {
        PathUtils.validatePath(path);
        this.path = path;
    }

    public String toString() {
        return path;
    }

    public boolean equals(final Object other) {
        return (other instanceof ZNode) && ((ZNode) other).path.equals(path);
    }

    public int hashCode() {
        return path.hashCode();
    }
}
