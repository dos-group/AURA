package de.tuberlin.aura.core.filesystem.out;

import de.tuberlin.aura.core.filesystem.in.CSVInputFormat;
import de.tuberlin.aura.core.record.tuples.AbstractTuple;
import org.apache.hadoop.fs.Path;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;


public class CSVOutputFormat<T extends AbstractTuple> extends FileOutputFormat<T> {
    private static final long serialVersionUID = 1L;

    // --------------------------------------------------------------------------------------------

    public static final String DEFAULT_LINE_DELIMITER = CSVInputFormat.DEFAULT_LINE_DELIMITER;

    public static final String DEFAULT_FIELD_DELIMITER = String.valueOf(CSVInputFormat.DEFAULT_FIELD_DELIMITER);

    // --------------------------------------------------------------------------------------------

    private transient Writer wrt;

    private String fieldDelimiter;

    private String recordDelimiter;

    private String charsetName;

    private boolean allowNullValues = true;

    private boolean quoteStrings = false;

    // --------------------------------------------------------------------------------------------
    // Constructors and getters/setters for the configurable parameters
    // --------------------------------------------------------------------------------------------

    public CSVOutputFormat(Path outputPath) {
        this(outputPath, DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER);
    }

    public CSVOutputFormat(Path outputPath, String fieldDelimiter) {
        this(outputPath, DEFAULT_LINE_DELIMITER, fieldDelimiter);
    }

    public CSVOutputFormat(Path outputPath, String recordDelimiter, String fieldDelimiter) {
        super(outputPath);
        if (recordDelimiter == null) {
            throw new IllegalArgumentException("RecordDelmiter shall not be null.");
        }

        if (fieldDelimiter == null) {
            throw new IllegalArgumentException("FieldDelimiter shall not be null.");
        }

        this.fieldDelimiter = fieldDelimiter;
        this.recordDelimiter = recordDelimiter;
        this.allowNullValues = false;
    }

    public void setAllowNullValues(boolean allowNulls) {
        this.allowNullValues = allowNulls;
    }

    public void setCharsetName(String charsetName) {
        this.charsetName = charsetName;
    }

    public void setQuoteStrings(boolean quoteStrings) {
        this.quoteStrings = quoteStrings;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);
        this.wrt = this.charsetName == null ? new OutputStreamWriter(new BufferedOutputStream(this.stream, 4096)) :
                new OutputStreamWriter(new BufferedOutputStream(this.stream, 4096), this.charsetName);
    }

    @Override
    public void close() throws IOException {
        if (wrt != null) {
            this.wrt.close();
        }
        super.close();
    }

    @Override
    public void writeRecord(T element) throws IOException {
        int numFields = element.length();

        for (int i = 0; i < numFields; i++) {
            Object v = element.getField(i);
            if (v != null) {
                if (i != 0) {
                    this.wrt.write(this.fieldDelimiter);
                }

                if (quoteStrings) {
                    if (v instanceof String /*|| v instanceof StringValue*/) {
                        this.wrt.write('"');
                        this.wrt.write(v.toString());
                        this.wrt.write('"');
                    } else {
                        this.wrt.write(v.toString());
                    }
                } else {
                    this.wrt.write(v.toString());
                }
            } else {
                if (this.allowNullValues) {
                    if (i != 0) {
                        this.wrt.write(this.fieldDelimiter);
                    }
                } else {
                    throw new RuntimeException("Cannot write tuple with <null> value at position: " + i);
                }
            }
        }

        // add the record delimiter
        this.wrt.write(this.recordDelimiter);
    }

    // --------------------------------------------------------------------------------------------
    @Override
    public String toString() {
        return "CsvOutputFormat (path: " + this.getOutputFilePath() + ", delimiter: " + this.fieldDelimiter + ")";
    }
}