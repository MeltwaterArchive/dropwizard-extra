package com.datasift.dropwizard.logging.gelf;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.Layout;
import ch.qos.logback.core.status.ErrorStatus;
import ch.qos.logback.core.status.WarnStatus;
import com.google.common.base.Charsets;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;

/**
 * An {@link ch.qos.logback.core.Appender} for sending to a remote GELF server.
 */
public class GelfAppender<E extends ILoggingEvent> extends AppenderBase<E> {

    private MessageDigest MD5;
    private Layout<E> layout;
    private SocketAddress target;
    private DatagramSocket socket;
    private String hostname = "localhost";
    private int port = 12201;
    private int chunkSize = 1024;

    public GelfAppender<E> setLayout(final Layout<E> layout) {
        this.layout = layout;
        return this;
    }

    public GelfAppender<E> setHostname(final String hostname) {
        this.hostname = hostname;
        return this;
    }

    public GelfAppender<E> setPort(final int port) {
        this.port = port;
        return this;
    }

    public GelfAppender<E> setChunkSize(final int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    @Override
    public void start() {
        try {
            MD5 = MessageDigest.getInstance("MD5");
        } catch (final NoSuchAlgorithmException e) {
            error("Unable to load MD5 implementation", e);
            return;
        }

        try {
            socket = new DatagramSocket();
        } catch (final SocketException e) {
            error("Failed to bind local UDP socket", e);
            return;
        }

        target = new InetSocketAddress(hostname, port);

        super.start();
    }

    @Override
    public void stop() {
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
        socket = null;

        super.stop();
    }

    @Override
    protected void append(final E event) {

        if (!isStarted()) {
            warn("Appender not started yet");
            return;
        }

        if (socket == null) {
            error("Appender not running (socket unavailable)");
            return;
        }

        // format event using GelfLayout
        final String data = layout.doLayout(event);

        try {
            // compress event with zlib/gzip
            final byte[] compressed = compress(data.getBytes(Charsets.UTF_8));

            // generate message header
            // todo: test/verify this algorithm
            final int frameSize = chunkSize - 12;
            final byte total = (byte) Math.ceil(compressed.length / frameSize);
            final byte[] header = new byte[] { 0x1e, 0x0f, 0, 0, 0, 0, 0, 0, 0, 0, 1, total };
            System.arraycopy(MD5.digest(compressed), 0, header, 2, 8);

            // send message chunks
            // todo: test/verify this algorithm
            final byte[] chunk = new byte[chunkSize];
            for (byte i = 0; i < total; i++) {
                final int offset = i * frameSize;
                final int length = Math.min(frameSize, compressed.length - offset);

                System.arraycopy(header, 0, chunk, 0, 12);
                System.arraycopy(compressed, offset, chunk, 12, length);
                chunk[10] = i;

                socket.send(new DatagramPacket(chunk, 12 + length, target));
            }

        } catch (final IOException e) {
            getContext().getStatusManager().add(new ErrorStatus(e.getMessage(), this, e));
        }
    }

    private byte[] compress(final byte[] data) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            try (GZIPOutputStream compressed = new GZIPOutputStream(out)) {
                compressed.write(data);
                compressed.close();
                out.close();
                return out.toByteArray();
            }
        }
    }

    private void warn(final String message) {
        getContext().getStatusManager().add(new WarnStatus(message, this));
    }

    private void error(final String message) {
        error(message, null);
    }

    private void error(final String message, final Throwable t) {
        getContext().getStatusManager().add(new ErrorStatus(message, this, t));
    }
}
