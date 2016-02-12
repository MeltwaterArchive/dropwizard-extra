package com.datasift.dropwizard.hbase.util;

import com.stumbleupon.async.Callback;
import com.codahale.metrics.Timer;

/**
 * A {@link com.stumbleupon.async.Callback} for stopping a {@link com.codahale.metrics.Timer.Context} on completion.
 */
public class TimerStoppingCallback<T> implements Callback<T, T> {

    /**
     * The context of the active {@link com.codahale.metrics.Timer} to stop.
     */
    private final Timer.Context timer;

    /**
     * Creates a new {@link Callback} that stops the given active timer on completion.
     *
     * @param timer the active {@link com.codahale.metrics.Timer} to stop on completion of the
     *              {@link Callback}.
     */
    public TimerStoppingCallback(final Timer.Context timer) {
        this.timer = timer;
    }

    /**
     * Stops the registered {@link com.codahale.metrics.Timer} and proxies any argument through
     * verbatim.
     *
     * @param arg the argument (if any) to pass-through.
     *
     * @return the argument (if any), proxied verbatim.
     *
     * @throws Exception if an error occurs stopping the {@link com.codahale.metrics.Timer}.
     */
    public T call(final T arg) throws Exception {
        timer.stop();
        return arg;
    }
}
