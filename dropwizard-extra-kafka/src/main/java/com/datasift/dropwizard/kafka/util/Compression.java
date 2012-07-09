package com.datasift.dropwizard.kafka.util;

import kafka.message.*;
import org.codehaus.jackson.annotate.JsonCreator;

/**
 * A utility for parsing {@link CompressionCodec}s from a {@link com.yammer.dropwizard.config.Configuration}.
 */
public class Compression {

    private CompressionCodec codec;

    public Compression(int codec) {
        this.codec = CompressionCodec$.MODULE$.getCompressionCodec(codec);
    }

    public CompressionCodec getCodec() {
        return codec;
    }

    @JsonCreator
    public static Compression parse(String codec) {
        if ("gzip".equals(codec) || "gz".equals(codec)) {
            return new Compression(GZIPCompressionCodec.codec());
        } else if ("none".equals(codec) || "no".equals(codec) || "false".equals(codec)) {
            return new Compression(NoCompressionCodec.codec());
        } else if ("default".equals(codec) || "null".equals(codec) || codec == null) {
            return new Compression(DefaultCompressionCodec.codec());
        } else {
            throw new IllegalArgumentException("Invalid Compression: " + codec);
        }
    }
}
