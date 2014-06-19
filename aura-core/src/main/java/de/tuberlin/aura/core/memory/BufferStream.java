package de.tuberlin.aura.core.memory;

import java.io.*;
import java.util.Arrays;

/**
 *
 */
public final class BufferStream {

    /**
     *
     */
    public static interface BufferInput {

        public abstract MemoryView get();
    }

    /**
     *
     */
    public static interface BufferOutput {

        public abstract void put(final MemoryView buffer);
    }

    /**
     *
     */
    public static class ContinuousByteOutputStream extends OutputStream {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        protected MemoryView buf;

        protected int count;

        protected BufferInput bufferInput;

        protected BufferOutput bufferOutput;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public ContinuousByteOutputStream() {
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        public void setBufferInput(final BufferInput bufferInput) {
            // sanity check.
            if (bufferInput == null)
                throw new IllegalArgumentException("bufferInput == null");

            this.bufferInput = bufferInput;
        }

        public void setBufferOutput(final BufferOutput bufferOutput) {
            // sanity check.
            if (bufferOutput == null)
                throw new IllegalArgumentException("bufferOutput == null");

            this.bufferOutput = bufferOutput;
        }

        public synchronized void write(int b) {

            if (buf == null) {
                nextBuf();
            }

            buf.memory[count] = (byte) b;
            count += 1;

            if (count >= buf.size) {

                nextBuf();
            }
        }

        public synchronized void write(byte b[], int off, int len) {
            if ((off < 0) || (off > b.length) || (len < 0) ||
                    ((off + len) - b.length > 0)) {
                throw new IndexOutOfBoundsException();
            }

            if (buf == null) {
                nextBuf();
            }

            //if (count + len > buf.size) {

                int copiedLen = 0;

                int originalLen = len;

                while (!(originalLen == copiedLen)) {

                    final int avail = buf.size - (count - buf.baseOffset);

                    if (originalLen - copiedLen > avail)
                        len = avail;
                    else
                        len = originalLen - copiedLen;

                    System.arraycopy(b, off, buf.memory, count, len);

                    off += len;

                    copiedLen += len;

                    if (!(originalLen == copiedLen)) {
                        nextBuf();
                    }
                }

                count += len;

                return;

            /*} else {

                System.arraycopy(b, off, buf.memory, count, len);

                count += len;

                if (count >= buf.size) {
                    nextBuf();
                }
            }*/
        }

        public synchronized void writeTo(OutputStream out) throws IOException {
            throw new UnsupportedOperationException();
        }

        public synchronized void reset() {
            count = 0;
        }

        public synchronized byte[] toByteArray() {
            return Arrays.copyOf(buf.memory, count);
        }

        public synchronized int size() {
            return count;
        }

        public synchronized String toString() {
            throw new UnsupportedOperationException();
        }

        public synchronized String toString(String charsetName)
                throws UnsupportedEncodingException {
            throw new UnsupportedOperationException();
        }

        @Deprecated
        public synchronized String toString(int hibyte) {
            throw new UnsupportedOperationException();
        }

        public void close() throws IOException {
            flush();
        }

        public void flush() throws IOException {
            if (buf != null && bufferOutput != null)
                bufferOutput.put(buf);
        }

        // ---------------------------------------------------
        // Private Methods.
        // ---------------------------------------------------

        private void nextBuf() {

            if (buf != null && bufferOutput != null)
                bufferOutput.put(buf);

            buf = bufferInput.get();

            if(buf == null) {
                System.out.println("STOP");
            }

            count = buf.baseOffset;
        }
    }

    /**
     *
     */
    public static class ContinuousByteInputStream extends InputStream {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        protected MemoryView buf;

        protected BufferInput bufferInput;

        protected BufferOutput bufferOutput;

        protected int pos;

        protected int mark = 0;

        protected int count;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public ContinuousByteInputStream() {
            this.buf = null;
            this.pos = 0;
            this.count = 0;
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        public void setBufferInput(final BufferInput bufferInput) {
            // sanity check.
            if (bufferInput == null)
                throw new IllegalArgumentException("bufferInput == null");

            this.bufferInput = bufferInput;
        }

        public void setBufferOutput(final BufferOutput bufferOutput) {
            // sanity check.
            if (bufferOutput == null)
                throw new IllegalArgumentException("bufferOutput == null");

            this.bufferOutput = bufferOutput;
        }

        public synchronized int read() {

            if ((pos - buf.baseOffset) < count) {

                return (buf.memory[pos++] & 0xff);

            } else {

                nextBuf();

                return (buf.memory[pos++] & 0xff);
            }
        }

        public synchronized int read(byte b[], int off, int len) {
            if (b == null) {
                throw new NullPointerException();
            } else if (off < 0 || len < 0 || len > b.length - off) {
                throw new IndexOutOfBoundsException();
            }

            if (buf == null) {
                nextBuf();
            }

            if ((pos - buf.baseOffset) > count) {
                nextBuf();
            }

            if (len <= 0) {
                return 0;
            }

            int avail = count - (pos - buf.baseOffset);

            if (len > avail) {

                int originalLen = len;

                int copiedLen = 0;

                while (!(originalLen == copiedLen)) {

                    if (originalLen - copiedLen > avail)
                        len = avail;
                    else
                        len = originalLen - copiedLen;

                    System.arraycopy(buf.memory, pos, b, off + copiedLen, len);

                    copiedLen += len;

                    if (!(originalLen == copiedLen)) {

                        int res = nextBuf();

                        if(res == -1)
                            return -1;

                        avail = count - (pos - buf.baseOffset);
                    }
                }

                pos += len;

                return copiedLen;

            } else {

                System.arraycopy(buf.memory, pos, b, off, len);

                pos += len;

                return len;
            }
        }

        public synchronized long skip(long n) {

            int avail = count - (pos - buf.baseOffset);

            if (n > avail) {

                int originalLen = (int)n;

                int skippedLen = 0;

                while (!(originalLen == skippedLen)) {

                    if (originalLen - skippedLen > avail)
                        n = avail;
                    else
                        n = originalLen - skippedLen;

                    skippedLen += n;

                    if (!(originalLen == skippedLen)) {

                        nextBuf();

                        avail = count - (pos - buf.baseOffset);
                    }
                }

                pos += n;

                return skippedLen;

            } else {

                pos += n;

                return n;
            }
        }

        public synchronized int available() {
            return count - (pos - buf.baseOffset);
        }

        public boolean markSupported() {
            return true;
        }

        public void mark(int readAheadLimit) {
            mark = pos;
        }

        public synchronized void reset() {
            pos = mark;
        }

        public void close() throws IOException {
            flush();
        }

        public void flush() throws IOException {
            if (buf != null && bufferOutput != null)
                bufferOutput.put(buf);
        }

        // ---------------------------------------------------
        // Private Methods.
        // ---------------------------------------------------

        private int nextBuf() {

            if (buf != null && bufferOutput != null)
                bufferOutput.put(buf);

            buf = bufferInput.get();

            if(buf == null)
                return -1;

            pos = buf.baseOffset;

            count = buf.size;

            return 0;
        }
    }
}