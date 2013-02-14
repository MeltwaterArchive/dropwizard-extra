package com.datasift.dropwizard.hbase.util;

import com.stumbleupon.async.Callback;
import com.yammer.metrics.core.TimerContext;

/**
 * A {@link Callback} for stopping a {@link TimerContext} on completion.
 */
public class TimerStoppingCallback<T> implements Callback<T, T> {

    /**
     * The context of the active {@link com.yammer.metrics.core.Timer} to stop.
     */
    private final TimerContext timer;

    /**
     * Creates a new {@link Callback} that stops the given active timer on completion.
     *
     * @param timer the active {@link com.yammer.metrics.core.Timer} to stop on completion of the
     *              {@link Callback}.
     */
    public TimerStoppingCallback(final TimerContext timer) {
        this.timer = timer;
    }

    /**
     * Stops the registered {@link com.yammer.metrics.core.Timer} and proxies any argument through
     * verbatim.
     *
     * @param arg the argument (if any) to pass-through.
     *
     * @return the argument (if any), proxied verbatim.
     *
     * @throws Exception if an error occurs stopping the {@link com.yammer.metrics.core.Timer}.
     */
    public T call(final T arg) throws Exception {
        timer.stop();
        return arg;
    }
}
