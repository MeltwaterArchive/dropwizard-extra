package com.datasift.dropwizard.hbase.util;

import com.stumbleupon.async.Callback;

import java.util.concurrent.Semaphore;

/**
 * A Deferred Callback that releases a permit on a given Semaphore.
 */
public class PermitReleasingCallback<T> implements Callback<T, T> {

    private Semaphore semaphore;

    public PermitReleasingCallback(Semaphore semaphore) {
        this.semaphore = semaphore;
    }

    public T call(T arg) throws Exception {
        semaphore.release();
        return arg;
    }
}
