package com.datasift.dropwizard.zookeeper.config;

import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.retry.RetryNTimes;
import com.netflix.curator.retry.RetryUntilElapsed;
import com.yammer.dropwizard.util.Duration;
import org.codehaus.jackson.annotate.JsonCreator;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * TODO: Document
 */
public class CuratorConfiguration {

    private static final Duration DEFAULT_SLEEP_DURATION = Duration.seconds(3);

    protected ZooKeeperConfiguration ensemble = new ZooKeeperConfiguration();

    protected RetryPolicy retryPolicy = new RetryPolicy("retry 5 times sleeping for 3 seconds");

    public ZooKeeperConfiguration getEnsembleConfiguration() {
        return ensemble;
    }

    public com.netflix.curator.RetryPolicy getRetryPolicy() {
        return retryPolicy.getPolicy();
    }

    class RetryPolicy {

        private final Pattern backoffPattern = Pattern.compile(
                "(?i)backoff\\s+(?:upto\\s+)?(\\d+)\\s+times(?:\\s+sleeping(?:\\s+for)?\\s+(\\d+\\s*\\w+))?");
        private final Pattern fixedRetryPattern = Pattern.compile(
                "(?i)(?:retry\\s+)?(\\d+)\\s+times(?:\\s+sleeping(?:\\s+for)?\\s+(\\d+\\s*\\w+))?");
        private final Pattern boundedRetryPattern = Pattern.compile(
                "(?i)(?:retry\\s+)?until\\s+(\\d+\\s*\\w+)(?:\\s+sleeping(?:\\s+for)?\\s+(\\d+\\s*\\w+))?");

        private final com.netflix.curator.RetryPolicy policy;

        @JsonCreator
        public RetryPolicy(final String value) {
            final Matcher backoffMatcher = backoffPattern.matcher(value);
            final Matcher fixedMatcher = fixedRetryPattern.matcher(value);
            final Matcher boundedMatcher = boundedRetryPattern.matcher(value);

            if (backoffMatcher.matches()) {
                final String sleep = backoffMatcher.group(2);
                final Duration sleepTime = sleep.isEmpty()
                        ? DEFAULT_SLEEP_DURATION
                        : Duration.parse(sleep);

                policy = new ExponentialBackoffRetry(
                        (int) sleepTime.toMilliseconds(), Integer.parseInt(backoffMatcher.group(1)));
            } else if (fixedMatcher.matches()) {
                final String sleep = fixedMatcher.group(2);
                final Duration sleepTime = sleep.isEmpty()
                        ? DEFAULT_SLEEP_DURATION
                        : Duration.parse(sleep);
                policy = new RetryNTimes(
                        Integer.parseInt(fixedMatcher.group(1)), (int) sleepTime.toMilliseconds());
            } else if (boundedMatcher.matches()) {
                final String sleep = boundedMatcher.group(2);
                final Duration sleepTime = sleep.isEmpty()
                        ? DEFAULT_SLEEP_DURATION
                        : Duration.parse(sleep);
                policy = new RetryUntilElapsed(
                        Integer.parseInt(boundedMatcher.group(1)), (int) sleepTime.toMilliseconds());
            }

            throw new IllegalArgumentException("Invalid RetryPolicy: " + value);
        }

        public com.netflix.curator.RetryPolicy getPolicy() {
            return policy;
        }
    }
}
