package com.datasift.dropwizard.kafka.compression;

import kafka.message.*;

/**
 * TODO: Document
 */
public class Compression {

    private CompressionCodec codec;

    public Compression(int codec) {
        this.codec = CompressionCodec$.MODULE$.getCompressionCodec(codec);
    }

    public CompressionCodec getCodec() {
        return codec;
    }

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
