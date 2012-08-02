package com.datasift.dropwizard.health;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.yammer.metrics.core.HealthCheck;
import com.yammer.metrics.core.HealthCheck.Result;

import java.util.List;
import java.util.Map;

/**
 * A {@link HealthCheck} for a quorum of ZooKeeper nodes.
 * <p>
 * Each node in the quorum is checked by its own {@link ZooKeeperHealthCheck},
 * but failures are grouped under the quorums'
 * {@link ZooKeeperQuorumHealthCheck}.
 * <p>
 * <b>
 * TODO: find a way to actually report the reason for failed node healthchecks
 * </b>
 */
public class ZooKeeperQuorumHealthCheck extends HealthCheck {

    private final List<ZooKeeperHealthCheck> healthChecks;

    /**
     * Builds a {@link ZooKeeperQuorumHealthCheck} for the given nodes.
     *
     * @param nodes the ZooKeeper nodes in the quorum as a mapping of host/port
     *              pairs
     * @param name  the name of the {@link ZooKeeperQuorumHealthCheck}
     * @return a {@link ZooKeeperQuorumHealthCheck} for the given nodes.
     */
    public static ZooKeeperQuorumHealthCheck forNodes(final Map<String, Integer> nodes,
                                                      final String name) {
        final ImmutableList.Builder<ZooKeeperHealthCheck> builder =
                ImmutableList.builder();
        for (final Map.Entry<String, Integer> endpoint : nodes.entrySet()) {
            final String host = endpoint.getKey();
            final int port = endpoint.getValue();
            builder.add(new ZooKeeperHealthCheck(host, port, name));
        }
        return new ZooKeeperQuorumHealthCheck(builder.build(), name);
    }

    /**
     * Builds a {@link ZooKeeperQuorumHealthCheck} for the given nodes.
     *
     * @param hosts the hostnames of the ZooKeeper nodes in the quorum
     * @param port  the port to connect to all the nodes on
     * @param name  the name of the {@link ZooKeeperQuorumHealthCheck}
     * @return a {@link ZooKeeperQuorumHealthCheck} for the given nodes.
     */
    public static ZooKeeperQuorumHealthCheck forNodes(final String[] hosts,
                                                      final int port,
                                                      final String name) {
        final ImmutableList.Builder<ZooKeeperHealthCheck> builder =
                ImmutableList.builder();
        for (final String host : hosts) {
            builder.add(new ZooKeeperHealthCheck(host, port, name));
        }
        return new ZooKeeperQuorumHealthCheck(builder.build(), name);
    }

    /**
     * Creates a {@link ZooKeeperQuorumHealthCheck} from the given array of
     * {@link ZooKeeperHealthCheck}.
     *
     * @param checks an array of {@link ZooKeeperHealthCheck} for each node in
     *               the quorum
     * @param name   the name of this {@link HealthCheck}
     */
    public ZooKeeperQuorumHealthCheck(final ZooKeeperHealthCheck[] checks,
                                      final String name) {
        this(ImmutableList.copyOf(checks), name);
    }

    /**
     * Creates a {@link ZooKeeperQuorumHealthCheck} from the given {@link List}
     * of {@link ZooKeeperHealthCheck}.
     *
     * @param checks a {@link List} of {@link ZooKeeperHealthCheck} for each
     *               node in the quorum
     * @param name   the name of this {@link HealthCheck}
     */
    public ZooKeeperQuorumHealthCheck(final List<ZooKeeperHealthCheck> checks,
                                      final String name) {
        super(name);
        this.healthChecks = checks;
    }

    /**
     * Checks the health of the ZooKeeper quorum.
     * <p>
     * The health of the quorum is dictated by the health of the underlying
     * nodes:
     * <dl>
     *     <dt>No failures:</dt>
     *     <dd>{@link Result#healthy() Healthy}</dd>
     *     <dt>Partial failures:</dt>
     *     <dd>{@link Result#unhealthy(String) Unhealthy}
     *     (with message indicating partial failure of quorum)</dd>
     *     <dt>Total failure:</dt>
     *     <dd>{@link Result#unhealthy(String) Unhealthy}
     *     (with message indicating total failure of quorum)</dd>
     * </dl>
     * <p>
     * A <i>partial failure</i> is when <i>some</i> nodes in the quorum are
     * unhealthy; a <i>total failure</i>, when <i>all</i> nodes are unhealthy.
     *
     * @return healthy if the entire quorum is healthy; unhealthy if any nodes
     *         in the quorum are unhealthy
     * @throws Exception an unexpected error occurred while checking the health
     *                   of the quorum
     */
    @Override
    protected Result check() throws Exception {
        final List<ZooKeeperHealthCheck> unhealthy = getUnhealthy();
        final Joiner joiner = Joiner.on(", ");

        if (unhealthy.isEmpty()) {
            return Result.healthy();
        } else if (unhealthy.size() < healthChecks.size()) {
            return Result.unhealthy(String.format(
                    "Some nodes are unhealthy: %s", joiner.join(unhealthy)));
        } else {
            return Result.unhealthy(String.format(
                    "All nodes are unhealthy: %s", joiner.join(unhealthy)));
        }
    }

    /**
     * Gets a {@link List} of the currently unhealthy nodes in the quorum.
     *
     * @return a {@link List} of unhealthy nodes in the quorum
     */
    protected List<ZooKeeperHealthCheck> getUnhealthy() {
        final ImmutableList.Builder<ZooKeeperHealthCheck> unhealthy =
                ImmutableList.builder();
        for (final ZooKeeperHealthCheck healthCheck : healthChecks) {
            try {
                if (!healthCheck.check().isHealthy()) {
                    unhealthy.add(healthCheck);
                }
            } catch (final Exception e) {
                unhealthy.add(healthCheck);
            }
        }
        return unhealthy.build();
    }
}
