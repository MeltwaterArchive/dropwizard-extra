package com.datasift.dropwizard.hbase.util;

import com.stumbleupon.async.Callback;

import java.util.concurrent.Semaphore;

/**
 * A {@link Callback} that releases a permit on a given {@link Semaphore}.
 */
public class PermitReleasingCallback<T> implements Callback<T, T> {

    /**
     * The {@link Semaphore} to release the permit to.
     */
    private Semaphore semaphore;

    /**
     * Creates a new {@link Callback} that releases a permit on the given
     * semaphore on completion.
     *
     * @param semaphore the {@link Semaphore} to release the permit to on
     *                  completion
     */
    public PermitReleasingCallback(Semaphore semaphore) {
        this.semaphore = semaphore;
    }

    /**
     * Releases a permit to the registered {@link Semaphore} and proxies any
     * argument through verbatim.
     *
     * @param arg the argument (if any) to pass-through
     * @return the argument (if any), proxied verbatim
     * @throws Exception if an error occurs releasing the permit to the
     *                   {@link Semaphore}
     */
    public T call(T arg) throws Exception {
        semaphore.release();
        return arg;
    }
}
