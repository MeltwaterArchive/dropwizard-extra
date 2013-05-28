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
            final byte[] compressed = compress(data);

            // generate message header
            // todo: test/verify this algorithm
            final byte total = (byte) Math.ceil(compressed.length / (chunkSize - 12));
            final byte[] header = new byte[] { 0x1e, 0x0f, 0, 0, 0, 0, 0, 0, 0, 0, 1, total };
            final byte[] id = Arrays.copyOf(MD5.digest(compressed), 8);
            System.arraycopy(id, 0, header, 2, 8);
            final byte[][] chunks = new byte[total][];

            // generate message chunks
            // todo: test/verify this algorithm
            for (byte i = 0; i < total; i++) {
                chunks[i] = new byte[chunkSize];
                System.arraycopy(header, 0, chunks[i], 0, 12);
                chunks[i][10] = i;
                System.arraycopy(compressed, i * (chunkSize - 12), chunks[i], 12, Math.min((chunkSize - 12), compressed.length - (i * (chunkSize - 12))));
            }

            // send chunks over the wire
            for (final byte[] chunk : chunks) {
                socket.send(new DatagramPacket(chunk, chunk.length, target));
            }

        } catch (final IOException e) {
            getContext().getStatusManager().add(new ErrorStatus(e.getMessage(), this, e));
        }
    }

    private byte[] compress(final String data) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            final GZIPOutputStream compressed = new GZIPOutputStream(out);
            try {
                compressed.write(data.getBytes(Charsets.UTF_8));
                compressed.close();
                out.close();
                return out.toByteArray();
            } finally {
                compressed.close();
            }
        } finally {
            out.close();
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
