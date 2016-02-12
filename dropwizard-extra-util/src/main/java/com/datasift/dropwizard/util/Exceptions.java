package com.datasift.dropwizard.util;

/**
 * Utilities for working with {@link Exception}s.
 */
public class Exceptions {

    /**
     * Creates a new {@link Exception} instance from the given {@link Class}, using the given args.
     * <p>
     * A new {@link Exception} instance object of the given {@link Class} is created, using
     * reflection, providing the given arguments to the constructor.
     *
     * @param clazz the {@link Class} of the {@link Exception} to instantiate.
     * @param args the arguments to pass to the constructor of the {@link Exception}.
     * @param <T> the type of the {@link Exception} to return.
     * @return a new instance of the given {@link Exception}, constructed using the given args.
     * @throws RuntimeException if there was a problem instantiating the {@link Exception}.
     */
    public static <T extends Exception> T newInstance(final Class<T> clazz, final Object... args) {
        try {
            return Classes.newInstance(clazz, args);
        } catch (final Exception e) { // TODO: be less lazy with the Exception handling?
            throw new RuntimeException("Exception while instantiating" + clazz, e);
        }
    }

    /**
     * Creates a new {@link Exception} instance of the same {@link Class} as the given
     * <i>template</i>, using the given constructor args.
     * <p>
     * A new {@link Exception} instance object of the given {@link Class} is created, using
     * reflection, providing the given arguments to the constructor.
     *
     * @param template an object that provides the {@link Class} of the {@link Exception } to
     *                 instantiate.
     * @param args the arguments to pass to the constructor of the {@link Exception}.
     * @param <T> the type of the {@link Exception} to return.
     * @return a new instance of the given {@link Exception}, constructed using the given args.
     * @throws RuntimeException if there was a problem instantiating the {@link Exception}.
     */
    public static <T extends Exception> T newInstanceFrom(final T template, final Object... args) {
        try {
            return Classes.newInstanceFrom(template, args);
        } catch (final Exception e) { // TODO: be less lazy with the Exception handling?
            throw new RuntimeException("Exception while instantiating" + template.getClass(), e);
        }
    }

    /**
     * Creates a new {@link Exception} instance from the given {@link Class}, using the given args,
     * ignoring visibility.
     * <p>
     * A new {@link Exception} instance object of the given {@link Class} is created, using
     * reflection, providing the given arguments to the constructor.
     * <p>
     * The visibility of the constructor defined by the arguments is ignored and a new instance
     * created irrespective of the defined visibility. This is potentially dangerous, as the API
     * likely makes no guarantee as to the behaviour when instantiating from a non-public
     * constructor.
     *
     * @param clazz the {@link Class} of the {@link Exception} to instantiate.
     * @param args the arguments to pass to the constructor of the {@link Exception}.
     * @param <T> the type of the {@link Exception} to return.
     * @return a new instance of the given {@link Exception}, constructed using the given args.
     * @throws RuntimeException if there was a problem instantiating the {@link Exception}.
     */
    public static <T extends Exception> T unsafeNewInstance(final Class<T> clazz,
                                                            final Object... args) {
        try {
            return Classes.unsafeNewInstance(clazz, args);
        } catch (final Exception e) { // TODO: be less lazy with the Exception handling?
            throw new RuntimeException("Exception while instantiating" + clazz, e);
        }
    }

    /**
     * Creates a new {@link Exception} instance of the same {@link Class} as the given
     * <i>template</i>, using the given constructor args, ignoring visibility.
     * <p>
     * A new {@link Exception} instance object of the given {@link Class} is created, using
     * reflection, providing the given arguments to the constructor.
     * <p>
     * The visibility of the constructor defined by the arguments is ignored and a new instance
     * created irrespective of the defined visibility. This is potentially dangerous, as the API
     * likely makes no guarantee as to the behaviour when instantiating from a non-public
     * constructor.
     *
     * @param template an object that provides the {@link Class} of the {@link Exception } to
     *                 instantiate.
     * @param args the arguments to pass to the constructor of the {@link Exception}.
     * @param <T> the type of the {@link Exception} to return.
     * @return a new instance of the given {@link Exception}, constructed using the given args.
     * @throws RuntimeException if there was a problem instantiating the {@link Exception}.
     */
    public static <T extends Exception> T unsafeNewInstanceFrom(final T template,
                                                                final Object... args) {
        try {
            return Classes.unsafeNewInstanceFrom(template, args);
        } catch (final Exception e) { // TODO: be less lazy with the Exception handling?
            throw new RuntimeException("Exception while instantiating" + template.getClass(), e);
        }
    }
}
