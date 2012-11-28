package com.datasift.dropwizard.zookeeper.config;

import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.retry.RetryNTimes;
import com.netflix.curator.retry.RetryUntilElapsed;
import com.yammer.dropwizard.util.Duration;
import org.codehaus.jackson.annotate.JsonCreator;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents and parses a Curator {@link RetryPolicy}.
 */
class RetryPolicy {

    private static final Duration DEFAULT_SLEEP_DURATION = Duration.seconds(3);

    private static final Pattern backoffPattern = Pattern.compile(
            "(?i)backoff\\s+(?:upto\\s+)?(\\d+)\\s+times(?:\\s+sleeping(?:\\s+for)?\\s+(\\d+\\s*\\w+))?");
    private static final Pattern fixedRetryPattern = Pattern.compile(
            "(?i)(?:retry\\s+)?(\\d+)\\s+times(?:\\s+sleeping(?:\\s+for)?\\s+(\\d+\\s*\\w+))?");
    private static final Pattern boundedRetryPattern = Pattern.compile(
            "(?i)(?:retry\\s+)?until\\s+(\\d+\\s*\\w+)(?:\\s+sleeping(?:\\s+for)?\\s+(\\d+\\s*\\w+))?");

    private final com.netflix.curator.RetryPolicy policy;

    public RetryPolicy(final com.netflix.curator.RetryPolicy policy) {
        this.policy = policy;
    }

    public com.netflix.curator.RetryPolicy getPolicy() {
        return policy;
    }

    public static RetryPolicy withBackOff(final int maxRetries,
                                          final Duration initialSleepDuration) {

        return new RetryPolicy(
                new ExponentialBackoffRetry(
                        (int) initialSleepDuration.toMilliseconds(),
                        maxRetries));
    }

    public static RetryPolicy times(final int retries, final Duration sleepDuration) {
        return new RetryPolicy(
                new RetryNTimes(
                        retries,
                        (int) sleepDuration.toMilliseconds()));
    }

    public static RetryPolicy until(final Duration maxDuration, final Duration sleepDuration) {
        return new RetryPolicy(
                new RetryUntilElapsed(
                        (int) maxDuration.toMilliseconds(),
                        (int) sleepDuration.toMilliseconds()));
    }

    @JsonCreator
    public static RetryPolicy parse(final String value) {
        final Matcher backoffMatcher = backoffPattern.matcher(value);
        final Matcher fixedMatcher = fixedRetryPattern.matcher(value);
        final Matcher untilMatcher = boundedRetryPattern.matcher(value);

        if (backoffMatcher.matches()) {
            final String sleep = backoffMatcher.group(2);
            return withBackOff(
                    Integer.parseInt(backoffMatcher.group(1)),
                    sleep.isEmpty() ? DEFAULT_SLEEP_DURATION : Duration.parse(sleep));
        } else if (fixedMatcher.matches()) {
            final String sleep = fixedMatcher.group(2);
            return times(
                    Integer.parseInt(fixedMatcher.group(1)),
                    sleep.isEmpty() ? DEFAULT_SLEEP_DURATION : Duration.parse(sleep));
        } else if (untilMatcher.matches()) {
            final String sleep = untilMatcher.group(2);
            return until(
                    Duration.parse(untilMatcher.group(1)),
                    sleep.isEmpty() ? DEFAULT_SLEEP_DURATION : Duration.parse(sleep));
        }

        throw new IllegalArgumentException("Invalid RetryPolicy: " + value);
    }
}
