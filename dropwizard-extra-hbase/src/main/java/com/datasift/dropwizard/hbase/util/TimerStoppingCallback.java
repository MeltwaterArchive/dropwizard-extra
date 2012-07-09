package com.datasift.dropwizard.hbase.util;

import com.stumbleupon.async.Callback;
import com.yammer.metrics.core.TimerContext;

/**
 * A {@link Callback} for stopping a {@link TimerContext} on completion.
 */
public class TimerStoppingCallback<T> implements Callback<T, T> {

    private TimerContext timer;

    public TimerStoppingCallback(TimerContext timer) {
        this.timer = timer;
    }

    public T call(T arg) throws Exception {
        timer.stop();
        return arg;
    }
}
