package de.tuberlin.aura.core.filesystem.in;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;

import de.tuberlin.aura.core.filesystem.FileInputSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.google.common.base.Charsets;

/**
 * Base implementation for input formats that split the input at a delimiter into records.
 * The parsing of the record bytes into the record has to be implemented in the
 * {@link #readRecord(Object, byte[], int, int)} method.
 * <p>
 * The default delimiter is the newline character {@code '\n'}.
 */
public abstract class DelimitedInputFormat<OT> extends FileInputFormat<OT> {

    private static final long serialVersionUID = 1L;

    // -------------------------------------- Constants -------------------------------------------

    /**
     * The log.
     */
    private static final Logger LOG = LoggerFactory.getLogger(DelimitedInputFormat.class);

    /**
     * The default read buffer size = 1MB.
     */
    private static final int DEFAULT_READ_BUFFER_SIZE = 1024 * 1024;

    // --------------------------------------------------------------------------------------------
    //  Variables for internal parsing.
    //  They are all transient, because we do not want them so be serialized
    // --------------------------------------------------------------------------------------------

    private transient byte[] readBuffer;

    private transient byte[] wrapBuffer;

    private transient int readPos;

    private transient int limit;

    private transient byte[] currBuffer;		// buffer in which current record byte sequence is found

    private transient int currOffset;			// offset in above buffer

    private transient int currLen;				// length of current byte sequence

    private transient boolean overLimit;

    private transient boolean end;


    // --------------------------------------------------------------------------------------------
    //  The configuration parameters. Configured on the instance and serialized to be shipped.
    // --------------------------------------------------------------------------------------------

    private byte[] delimiter = new byte[] {'\n'};

    private int lineLengthLimit = Integer.MAX_VALUE;

    private int bufferSize = -1;

    // --------------------------------------------------------------------------------------------
    //  Constructors & Getters/setters for the configurable parameters
    // --------------------------------------------------------------------------------------------

    public DelimitedInputFormat() {
        super();
    }

    protected DelimitedInputFormat(Path filePath) {
        super(filePath);
    }

    public byte[] getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(byte[] delimiter) {
        if (delimiter == null) {
            throw new IllegalArgumentException("Delimiter must not be null");
        }

        this.delimiter = delimiter;
    }

    public void setDelimiter(char delimiter) {
        setDelimiter(String.valueOf(delimiter));
    }

    public void setDelimiter(String delimiter) {
        setDelimiter(delimiter, Charsets.UTF_8);
    }

    public void setDelimiter(String delimiter, String charsetName) throws IllegalCharsetNameException, UnsupportedCharsetException {
        if (charsetName == null) {
            throw new IllegalArgumentException("Charset name must not be null");
        }

        Charset charset = Charset.forName(charsetName);
        setDelimiter(delimiter, charset);
    }

    public void setDelimiter(String delimiter, Charset charset) {
        if (delimiter == null) {
            throw new IllegalArgumentException("Delimiter must not be null");
        }
        if (charset == null) {
            throw new IllegalArgumentException("Charset must not be null");
        }

        this.delimiter = delimiter.getBytes(charset);
    }

    public int getLineLengthLimit() {
        return lineLengthLimit;
    }

    public void setLineLengthLimit(int lineLengthLimit) {
        if (lineLengthLimit < 1) {
            throw new IllegalArgumentException("Line length limit must be at least 1.");
        }

        this.lineLengthLimit = lineLengthLimit;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        if (bufferSize < 1) {
            throw new IllegalArgumentException("Buffer size must be at least 1.");
        }

        this.bufferSize = bufferSize;
    }

    // --------------------------------------------------------------------------------------------
    //  User-defined behavior
    // --------------------------------------------------------------------------------------------

    public abstract OT readRecord(OT reuse, byte[] bytes, int offset, int numBytes) throws IOException;

    // --------------------------------------------------------------------------------------------
    //  Pre-flight: Configuration, Splits, Sampling
    // --------------------------------------------------------------------------------------------

    @Override
    public void configure(Configuration parameters) {
        super.configure(parameters);
        setDelimiter(",");

        /*String delimString = parameters.getString(RECORD_DELIMITER, null);
        if (delimString != null) {
            String charsetName = parameters.getString(RECORD_DELIMITER_ENCODING, null);

            if (charsetName == null) {
                setDelimiter(delimString);
            } else {
                try {
                    setDelimiter(delimString, charsetName);
                }
                catch (UnsupportedCharsetException e) {
                    throw new IllegalArgumentException("The charset with the name '" + charsetName +
                            "' is not supported on this TaskManager instance.", e);
                }
            }
        }*/
    }

    @Override
    public void open(FileInputSplit split) throws IOException {
        super.open(split);

        this.bufferSize = this.bufferSize <= 0 ? DEFAULT_READ_BUFFER_SIZE : this.bufferSize;

        if (this.readBuffer == null || this.readBuffer.length != this.bufferSize) {
            this.readBuffer = new byte[this.bufferSize];
        }
        if (this.wrapBuffer == null || this.wrapBuffer.length < 256) {
            this.wrapBuffer = new byte[256];
        }

        this.readPos = 0;
        this.limit = 0;
        this.overLimit = false;
        this.end = false;

        if (this.splitStart != 0) {
            this.stream.seek(this.splitStart);
            readLine();

            // if the first partial record already pushes the stream over the limit of our split, then no
            // record starts within this split
            if (this.overLimit) {
                this.end = true;
            }
        } else {
            fillBuffer();
        }
    }

    @Override
    public boolean reachedEnd() {
        return this.end;
    }

    @Override
    public OT nextRecord(OT record) throws IOException {
        if (readLine()) {
            return readRecord(record, this.currBuffer, this.currOffset, this.currLen);
        } else {
            this.end = true;
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        this.wrapBuffer = null;
        this.readBuffer = null;
        super.close();
    }

    // --------------------------------------------------------------------------------------------

    protected final boolean readLine() throws IOException {
        if (this.stream == null || this.overLimit) {
            return false;
        }

        int countInWrapBuffer = 0;

		/* position of matching positions in the delimiter byte array */
        int i = 0;

        while (true) {
            if (this.readPos >= this.limit) {
                if (!fillBuffer()) {
                    if (countInWrapBuffer > 0) {
                        setResult(this.wrapBuffer, 0, countInWrapBuffer);
                        return true;
                    } else {
                        return false;
                    }
                }
            }

            int startPos = this.readPos;
            int count = 0;

            while (this.readPos < this.limit && i < this.delimiter.length) {
                if ((this.readBuffer[this.readPos++]) == this.delimiter[i]) {
                    i++;
                } else {
                    i = 0;
                }

            }

            // check why we dropped out
            if (i == this.delimiter.length) {
                // line end
                count = this.readPos - startPos - this.delimiter.length;

                // copy to byte array
                if (countInWrapBuffer > 0) {
                    // check wrap buffer size
                    if (this.wrapBuffer.length < countInWrapBuffer + count) {
                        final byte[] nb = new byte[countInWrapBuffer + count];
                        System.arraycopy(this.wrapBuffer, 0, nb, 0, countInWrapBuffer);
                        this.wrapBuffer = nb;
                    }
                    if (count >= 0) {
                        System.arraycopy(this.readBuffer, 0, this.wrapBuffer, countInWrapBuffer, count);
                    }
                    setResult(this.wrapBuffer, 0, countInWrapBuffer + count);
                    return true;
                } else {
                    setResult(this.readBuffer, startPos, count);
                    return true;
                }
            } else {
                count = this.limit - startPos;

                // check against the maximum record length
                if ( ((long) countInWrapBuffer) + count > this.lineLengthLimit) {
                    throw new IOException("The record length exceeded the maximum record length (" +
                            this.lineLengthLimit + ").");
                }

                // buffer exhausted
                if (this.wrapBuffer.length - countInWrapBuffer < count) {
                    // reallocate
                    byte[] tmp = new byte[Math.max(this.wrapBuffer.length * 2, countInWrapBuffer + count)];
                    System.arraycopy(this.wrapBuffer, 0, tmp, 0, countInWrapBuffer);
                    this.wrapBuffer = tmp;
                }

                System.arraycopy(this.readBuffer, startPos, this.wrapBuffer, countInWrapBuffer, count);
                countInWrapBuffer += count;
            }
        }
    }

    private final void setResult(byte[] buffer, int offset, int len) {
        this.currBuffer = buffer;
        this.currOffset = offset;
        this.currLen = len;
    }

    private final boolean fillBuffer() throws IOException {
        // special case for reading the whole split.
        if(this.splitLength == READ_WHOLE_SPLIT_FLAG) {
            int read = this.stream.read(this.readBuffer, 0, readBuffer.length);
            if (read == -1) {
                this.stream.close();
                this.stream = null;
                return false;
            } else {
                this.readPos = 0;
                this.limit = read;
                return true;
            }
        }
        // else ..
        int toRead = this.splitLength > this.readBuffer.length ? this.readBuffer.length : (int) this.splitLength;
        if (this.splitLength <= 0) {
            toRead = this.readBuffer.length;
            this.overLimit = true;
        }

        int read = this.stream.read(this.readBuffer, 0, toRead);

        if (read == -1) {
            this.stream.close();
            this.stream = null;
            return false;
        } else {
            this.splitLength -= read;
            this.readPos = 0;
            this.limit = read;
            return true;
        }
    }
}