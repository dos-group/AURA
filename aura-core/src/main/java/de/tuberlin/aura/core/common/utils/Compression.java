package de.tuberlin.aura.core.common.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public final class Compression {

    public static byte[] compress(final byte[] data) {
        // sanity check.
        if (data == null)
            throw new IllegalArgumentException("data == null");

        final Deflater deflater = new Deflater();
        deflater.setInput(data);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length);
        deflater.finish();
        final byte[] buffer = new byte[1024];
        while (!deflater.finished()) {
            int count = deflater.deflate(buffer); // returns the generated code... index
            baos.write(buffer, 0, count);
        }
        try {
            baos.close();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        final byte[] output = baos.toByteArray();
        deflater.end();
        return output;
    }

    public static byte[] decompress(final byte[] data) {
        // sanity check.
        if (data == null)
            throw new IllegalArgumentException("data == null");

        final Inflater inflater = new Inflater();
        inflater.setInput(data);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length);
        byte[] buffer = new byte[1024];
        try {
            while (!inflater.finished()) {
                int count = inflater.inflate(buffer);
                baos.write(buffer, 0, count);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        } finally {
            try {
                baos.close();
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
        final byte[] output = baos.toByteArray();
        inflater.end();
        return output;
    }
}
