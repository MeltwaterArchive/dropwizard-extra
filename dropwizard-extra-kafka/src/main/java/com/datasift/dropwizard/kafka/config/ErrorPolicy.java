package com.datasift.dropwizard.kafka.config;

import com.yammer.dropwizard.util.Duration;
import org.codehaus.jackson.annotate.JsonCreator;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A policy for defining the action to take in the event of an error.
 * TODO: move to consumer package?
 */
public class ErrorPolicy {

    public static final ErrorPolicy DEFAULT =
            new ErrorPolicy(ErrorAction.SHUTDOWN, Duration.seconds(0));

    public enum ErrorAction { SHUTDOWN, RESTART }

    private static final Pattern PATTERN =
            Pattern.compile("(shutdown|stop|restart|reboot)(?:\\s+after\\s+([\\w\\s]+))?");

    private ErrorAction action;
    private Duration delay;

    protected ErrorPolicy(ErrorAction action, Duration delay) {
        this.action = action;
        this.delay = delay;
    }

    public ErrorAction getAction() {
        return action;
    }

    public Duration getDelay() {
        return delay;
    }

    @JsonCreator
    public static ErrorPolicy parse(String policy) {
        Matcher matcher = PATTERN.matcher(policy);

        // ensure the policy is valid
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid ErrorPolicy: " + policy);
        }

        return new ErrorPolicy(parseAction(matcher.group(1)), parseDelay(matcher.group(2)));
    }

    private static ErrorAction parseAction(String action) {
        if ("shutdown".equals(action) || "stop".equals(action)) {
            return ErrorAction.SHUTDOWN;
        } else if ("restart".equals(action) || "reboot".equals(action)) {
            return ErrorAction.RESTART;
        } else {
            throw new IllegalArgumentException("Invalid ErrorAction: " + action);
        }
    }

    private static Duration parseDelay(String delay) {
        return Duration.parse(delay);
    }
}
