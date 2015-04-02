package com.datasift.dropwizard.kafka.util;

import kafka.message.*;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

/**
 * Tests {@link Compression}
 */
public class CompressionTest {

    @Test
    public void parsesGZIP() {
        assertCompression(GZIPCompressionCodec$.MODULE$, "gz");
        assertCompression(GZIPCompressionCodec$.MODULE$, "gzip");
    }

    @Test
    public void parseNoCodec() {
        assertCompression(NoCompressionCodec$.MODULE$, "none");
        assertCompression(NoCompressionCodec$.MODULE$, "no");
        assertCompression(NoCompressionCodec$.MODULE$, "false");
    }

    @Test
    public void parseDefaultCodec() {
        assertCompression(GZIPCompressionCodec$.MODULE$, "default");
        assertCompression(GZIPCompressionCodec$.MODULE$, "yes");
        assertCompression(GZIPCompressionCodec$.MODULE$, "null");
        assertCompression(GZIPCompressionCodec$.MODULE$, null);
    }

    private void assertCompression(final CompressionCodec expected, final String value) {
        assertThat(String.format("'%s' parses as %s", value, expected.getClass().getSimpleName()),
                Compression.parse(value).getCodec(),
                is(expected));
    }
}
