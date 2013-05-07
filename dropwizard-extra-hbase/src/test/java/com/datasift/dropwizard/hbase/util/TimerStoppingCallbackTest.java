package com.datasift.dropwizard.hbase.util;

import com.codahale.metrics.Clock;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Tests {@link TimerStoppingCallback}.
 */
public class TimerStoppingCallbackTest {

    private MetricRegistry registry;
    private Timer timer;

    @Before
    public void setUp() throws Exception {
        this.registry = new MetricRegistry() {
            @Override
            public Timer timer(final String name) {
                return new Timer(new ExponentiallyDecayingReservoir(), new Clock() {

                    private long val = 0;

                    @Override
                    public long getTick() {
                        return val += 50;
                    }
                });
            }
        };
        this.timer = registry.timer("test");
    }

    @Test
    public void returnsArg() throws Exception {
        final String arg = "test";
        final Timer.Context context = registry.timer("test").time();
        assertThat("callback returns argument",
                new TimerStoppingCallback<String>(context).call(arg), is(arg));
    }

    @Test
    public void stopsTimer() throws Exception {
        final Timer.Context ctx = timer.time();

        new TimerStoppingCallback<>(ctx).call(new Object());

        assertThat("timer has 1 timed value", timer.getCount(), is(1L));

        assertThat("timer recorded duration of call", timer.getSnapshot().getMax(), is(50L));
    }
}
