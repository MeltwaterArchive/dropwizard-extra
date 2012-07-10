package com.datasift.dropwizard.hbase.util;

import org.junit.Test;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import java.util.concurrent.Semaphore;

/**
 * Tests {@link PermitReleasingCallback}
 */
public class PermitReleasingCallbackTest {

    @Test
    public void returnsArg() throws Exception {
        String arg = "test";
        assertEquals("callback must return its argument untouched",
                arg, new PermitReleasingCallback<String>(new Semaphore(1)).call(arg));
    }

    @Test
    public void releasesPermit() throws Exception {
        Semaphore semaphore = new Semaphore(1);
        assertThat("semaphore begins with single permit",
                semaphore.availablePermits(), is(1));
        semaphore.acquire(1);
        assertThat("semaphore has no available permits",
                semaphore.availablePermits(), is(0));
        new PermitReleasingCallback<Object>(semaphore).call(new Object());
        assertThat("callback releases a permit",
                semaphore.availablePermits(), is(1));
    }
}
