package com.datasift.dropwizard.hbase.util;

import org.junit.Test;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.Semaphore;

/**
 * Tests {@link PermitReleasingCallback}.
 */
public class PermitReleasingCallbackTest {

    @Test
    public void returnsArg() throws Exception {
        final String arg = "test";
        assertThat("callback returns its argument",
                new PermitReleasingCallback<String>(new Semaphore(1)).call(arg),
                is(arg));
    }

    @Test
    public void releasesPermit() throws Exception {
        final Semaphore semaphore = new Semaphore(1);
        assertThat("semaphore begins with single permit",
                semaphore.availablePermits(), is(1));
        semaphore.acquire(1);
        assertThat("semaphore has no available permits",
                semaphore.availablePermits(), is(0));
        new PermitReleasingCallback<>(semaphore).call(new Object());
        assertThat("callback releases a permit",
                semaphore.availablePermits(), is(1));
    }
}
