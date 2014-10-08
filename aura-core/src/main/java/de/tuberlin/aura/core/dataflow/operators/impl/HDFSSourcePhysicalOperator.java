package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.filesystem.FileInputSplit;
import de.tuberlin.aura.core.filesystem.in.CSVInputFormat;
import de.tuberlin.aura.core.filesystem.in.InputFormat;
import de.tuberlin.aura.core.record.tuples.AbstractTuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.net.URI;


public class HDFSSourcePhysicalOperator<O> extends AbstractUnaryPhysicalOperator<Object,O> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private FileInputSplit split;

    private InputFormat<AbstractTuple, FileInputSplit> inputFormat;

    private AbstractTuple record;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public HDFSSourcePhysicalOperator(final IExecutionContext context) {
        super(context, null);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() throws Throwable {
        super.open();

        //inputFormat = (InputFormat<AbstractTuple, FileInputSplit>) getContext().getProperties().config.get("INPUT_FORMAT");

        final Path path = new Path((String)getContext().getProperties().config.get("HDFS_PATH"));

        final Class<?>[] fieldTypes = (Class<?>[]) getContext().getProperties().config.get("FIELD_TYPES");

        inputFormat = new CSVInputFormat(path, fieldTypes);

        final Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "hdfs://localhost:9000/");

        inputFormat.configure(conf);

        record = AbstractTuple.createTuple(((CSVInputFormat<AbstractTuple>)inputFormat).getFieldTypes().length);

        split = (FileInputSplit)getContext().getRuntime().getNextInputSplit();

        inputFormat.open(split);
    }

    @Override
    public O next() throws Throwable {
        inputFormat.nextRecord(record);
        return (O) record;
    }

    @Override
    public void close() throws Throwable {
        super.close();

        inputFormat.close();
    }

    @Override
    public void accept(final IVisitor<IPhysicalOperator> visitor) {
        visitor.visit(this);
    }
}
