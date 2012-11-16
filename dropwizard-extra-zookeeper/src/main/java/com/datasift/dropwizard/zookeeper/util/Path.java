package com.datasift.dropwizard.zookeeper.util;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Simple utility for parsing/validating a path in ZooKeeper.
 * <p/>
 * Validates ZNode paths against the rules specified in the <a
 * href="http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#ch_zkDataModel">
 * ZooKeeper Data Model</a>.
 */
public class Path {

    private final String path;

    /**
     * Create a ZNode {@link Path} from the given {@link String}.
     * <p/>
     * Trailing slashes in the path will be truncated to ensure all paths are
     * consistent.
     *
     * @param path the path as a {@link String}.
     * @throws IllegalArgumentException if the path is invalid.
     */
    public Path(final String path) {
        checkArgument(path.startsWith("/"), "Invalid path: %s", path);
        checkArgument(!(path + "/").contains("/./"),
                "Invalid path: %s (contains invalid '/.' component)", path);
        checkArgument(!(path + "/").contains("/../"),
                "Invalid path: %s (contains invalid '/..' component)", path);
        checkArgument(!(path + "/").contains("/zookeeper/"),
                "Invalid path: %s (contains reserved '/zookeeper' component)", path);

        // check for invalid Unicode code-points
        for (int i = 0; i < path.codePointCount(0, path.length()); i++) {
            final int invalidCodePoint = isInvalidCodePoint(path.codePointAt(i));
            checkArgument(invalidCodePoint == -1,
                    "Invalid path: %s (contains invalid Unicode code point: \\u%5x)",
                    path, invalidCodePoint);
        }

        this.path = path.replaceAll("/$", "");
    }

    public String toString() {
        return path;
    }

    /**
     * Rules for invalid Unicode code-points in ZooKeeper znode paths.
     */
    private int isInvalidCodePoint(final int codePoint) {
        return codePoint <= 0x0019 || // u0000 AND u0001 - u0019
                (codePoint >= 0x007F && codePoint <= 0x009F) || // u007F - u009F
                (codePoint >= 0xd800 && codePoint <= 0xF8FFF) || // ud800 - uF8FFF
                (codePoint >= 0xFFF0 && codePoint <= 0xFFFF) || // uFFF0 - uFFFF
                (codePoint >> 20 == 0xF) || // uF0000 - uFFFFF
                ((codePoint >> 20 > 0 && codePoint >> 20 < 0xF) && ((codePoint & 0xFFFE) == 0xFFFE))
                // uXFFFE - uXFFFF (1 <= X <= E)
                    ? codePoint : -1;
    }
}
