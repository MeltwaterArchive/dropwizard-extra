package com.datasift.dropwizard.hbase.util;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Tests {@link TimerStoppingCallback}.
 */
public class TimerStoppingCallbackTest {

    private MetricsRegistry registry;
    private Timer timer;

    @Before
    public void setUp() throws Exception {
        this.registry = new MetricsRegistry(new Clock() {

            private long val = 0;

            @Override
            public long tick() {
                return val += 50000000;
            }
        });
        this.timer = registry.newTimer(getClass(), "test");
    }

    @After
    public void tearDown() throws Exception {
        registry.shutdown();
    }

    @Test
    public void returnsArg() throws Exception {
        final String arg = "test";
        final TimerContext context = Metrics.newTimer(getClass(), "test").time();
        assertThat("callback returns argument",
                new TimerStoppingCallback<String>(context).call(arg), is(arg));
    }

    @Test
    public void stopsTimer() throws Exception {
        final TimerContext ctx = timer.time();

        new TimerStoppingCallback<Object>(ctx).call(new Object());

        assertThat("timer has 1 timed value",
                timer.count(), is(1L));

        assertThat("timer recorded duration of call",
                timer.max(), is(closeTo(50.0, 0.001)));
    }
}
