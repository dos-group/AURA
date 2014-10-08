package de.tuberlin.aura.core.filesystem.in;

import java.io.IOException;

import de.tuberlin.aura.core.filesystem.FileInputSplit;
import de.tuberlin.aura.core.filesystem.in.GenericCSVInputFormat;
import de.tuberlin.aura.core.filesystem.parser.FieldParser;
import de.tuberlin.aura.core.record.tuples.AbstractTuple;


import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;


public class CSVInputFormat<OUT extends AbstractTuple> extends GenericCSVInputFormat<OUT> {

    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_LINE_DELIMITER = "\n";

    public static final char DEFAULT_FIELD_DELIMITER = ',';

    private transient Object[] parsedValues;

    // To speed up readRecord processing. Used to find windows line endings.
    // It is set when open so that readRecord does not have to evaluate it
    private boolean lineDelimiterIsLinebreak = false;

    public CSVInputFormat(Path filePath) {
        super(filePath);
    }

    public CSVInputFormat(Path filePath, Class<?>... types) {
        this(filePath, DEFAULT_LINE_DELIMITER, DEFAULT_FIELD_DELIMITER, types);
    }

    public CSVInputFormat(Path filePath, String lineDelimiter, char fieldDelimiter, Class<?>... types) {
        super(filePath);
        setDelimiter(lineDelimiter);
        setFieldDelimiter(fieldDelimiter);
        setFieldTypes(types);
    }

    public void setFieldTypes(Class<?>... fieldTypes) {
        if (fieldTypes == null || fieldTypes.length == 0) {
            throw new IllegalArgumentException("Field types must not be null or empty.");
        }

        setFieldTypesGeneric(fieldTypes);
    }

    public void setFields(int[] sourceFieldIndices, Class<?>[] fieldTypes) {
        Preconditions.checkNotNull(sourceFieldIndices);
        Preconditions.checkNotNull(fieldTypes);

        setFieldsGeneric(sourceFieldIndices, fieldTypes);
    }

    public void setFields(boolean[] sourceFieldMask, Class<?>[] fieldTypes) {
        Preconditions.checkNotNull(sourceFieldMask);
        Preconditions.checkNotNull(fieldTypes);

        setFieldsGeneric(sourceFieldMask, fieldTypes);
    }

    public Class<?>[] getFieldTypes() {
        return super.getGenericFieldTypes();
    }

    @Override
    public void open(FileInputSplit split) throws IOException {
        super.open(split);

        //@SuppressWarnings("unchecked")
        FieldParser<Object>[] fieldParsers = (FieldParser<Object>[]) getFieldParsers();

        //throw exception if no field parsers are available
        if (fieldParsers.length == 0) {
            throw new IOException("CsvInputFormat.open(FileInputSplit split) - no field parsers to parse input");
        }

        // create the value holders
        this.parsedValues = new Object[fieldParsers.length];
        for (int i = 0; i < fieldParsers.length; i++) {
            this.parsedValues[i] = fieldParsers[i].createValue();
        }

        // left to right evaluation makes access [0] okay
        // this marker is used to fasten up readRecord, so that it doesn't have to check each call if the line ending is set to default
        if (this.getDelimiter().length == 1 && this.getDelimiter()[0] == '\n') {
            this.lineDelimiterIsLinebreak = true;
        }
    }

    @Override
    public OUT readRecord(OUT reuse, byte[] bytes, int offset, int numBytes) {
		/*
		 * Fix to support windows line endings in CSVInputFiles with standard delimiter setup = \n
		 */
        //Find windows end line, so find carriage return before the newline
        if (this.lineDelimiterIsLinebreak == true && numBytes > 0 && bytes[offset + numBytes - 1] == '\r') {
            //reduce the number of bytes so that the Carriage return is not taken as data
            numBytes--;
        }

        if (parseRecord(parsedValues, bytes, offset, numBytes)) {
            // valid parse, map values into pact record
            for (int i = 0; i < parsedValues.length; i++) {
                reuse.setField(parsedValues[i], i);
            }
            return reuse;
        } else {
            return null;
        }
    }


    @Override
    public String toString() {
        return "";
        //return "CSV Input (" + StringUtils.showControlCharacters(String.valueOf(getFieldDelimiter())) + ") " + getFilePath();
    }

    // -------------------------------------------------------------------------------------------
}
