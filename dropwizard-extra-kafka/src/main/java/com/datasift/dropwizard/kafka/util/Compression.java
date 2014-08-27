package com.datasift.dropwizard.kafka.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import kafka.message.*;
import kafka.message.DefaultCompressionCodec;
import kafka.message.GZIPCompressionCodec;
import kafka.message.NoCompressionCodec;

/**
 * A utility for parsing {@link CompressionCodec}s from a {@link
 * io.dropwizard.Configuration}.
 * <p/>
 * To create {@link Compression} instances, use {@link Compression#parse(String)} to parse an
 * instance from a {@link String}.
 * <p/>
 * This is provided to parse textual specifications of a {@link CompressionCodec}, for example in a
 * {@link io.dropwizard.Configuration}.
 */
public class Compression {

    private final CompressionCodec codec;

    /**
     * Creates a {@link Compression} instance for the given codec type.
     * <p/>
     * The valid codec values are defined by {@link CompressionCodec}.
     * <p/>
     * To create {@link Compression} instances, use the {@link Compression#parse(String)} factory
     * method to parse an instance from a {@link String}.
     *
     * @param codec the codec to use, as an integer index.
     *
     * @see Compression#parse(String)
     */
    private Compression(final int codec) {
        this.codec = CompressionCodec$.MODULE$.getCompressionCodec(codec);
    }

    /**
     * Gets the {@link CompressionCodec} instance for this {@link Compression}.
     *
     * @return the {@link CompressionCodec} instance for this {@link Compression}
     */
    public CompressionCodec getCodec() {
        return codec;
    }

    /**
     * Parses a String representation of a {@link CompressionCodec}.
     *
     * @param codec the name of the {@link CompressionCodec} to parse.
     *
     * @return a {@link Compression} instance for the codec.
     *
     * @throws IllegalArgumentException if codec is not a valid {@link CompressionCodec}.
     */
    @JsonCreator
    public static Compression parse(final String codec) {
        if ("gzip".equals(codec) || "gz".equals(codec)) {
            return new Compression(GZIPCompressionCodec.codec());
        } else if ("snappy".equals(codec)) {
            return new Compression(SnappyCompressionCodec.codec());
        } else if ("none".equals(codec) || "no".equals(codec) || "false".equals(codec)) {
            return new Compression(NoCompressionCodec.codec());
        } else if ("default".equals(codec)
                || "yes".equals(codec)
                || "null".equals(codec)
                || codec == null)
        {
            return new Compression(DefaultCompressionCodec.codec());
        } else {
            throw new IllegalArgumentException("Invalid Compression: " + codec);
        }
    }
}
