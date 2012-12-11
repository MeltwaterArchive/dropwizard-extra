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
     * @param path the path of the ZNode as a {@link String}.
     * @throws IllegalArgumentException if the path is invalid for a ZNode.
     * @see PathUtils#validatePath(String);
     */
    public ZNode(final String path) {
        PathUtils.validatePath(path);
        this.path = path;
    }

    /**
     * Gets the path of this ZNode as a String.
     *
     * @return The path of this ZNode as a String.
     */
    public String toString() {
        return path;
    }

    /**
     * Tests if this ZNode is equal to another {@link Object}.
     * <p/>
     * ZNodes are only comparable to other ZNodes. Attempting to compare with any other {@link
     * Object} will always yield <code>false</code>.
     * <p/>
     * Two ZNodes are equal only if the underlying {@code path}s match.
     *
     * @param other the object to compare to this ZNode
     * @return {@code true} if the object is a ZNode that represents the same node as this one.
     */
    public boolean equals(final Object other) {
        return (other instanceof ZNode) && other.toString().equals(toString());
    }

    public int hashCode() {
        return path.hashCode();
    }
}
