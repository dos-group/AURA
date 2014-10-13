package de.tuberlin.aura.core.dataflow.operators.impl;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractUnaryPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.filesystem.out.CSVOutputFormat;
import de.tuberlin.aura.core.filesystem.out.OutputFormat;
import de.tuberlin.aura.core.record.OperatorResult;
import de.tuberlin.aura.core.record.tuples.AbstractTuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


public class HDFSSinkPhysicalOperator <I> extends AbstractUnaryPhysicalOperator<I,I> {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String HDFS_SINK_FILE_PATH = "HDFS_SINK_FILE_PATH";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private OutputFormat<AbstractTuple> outputFormat;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public HDFSSinkPhysicalOperator(final IExecutionContext context, final IPhysicalOperator<I> inputOp) {
        super(context, inputOp);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void open() throws Throwable {
        super.open();

        final Path path = new Path((String)getContext().getProperties().config.get(HDFS_SINK_FILE_PATH));

        this.outputFormat = new CSVOutputFormat<AbstractTuple>(path);

        final Configuration conf = new Configuration();

        conf.set("fs.defaultFS", getContext().getRuntime().getTaskManager().getConfig().getString("tm.io.hdfs.hdfs_url"));

        this.outputFormat.configure(conf);

        this.outputFormat.open(getContext().getNodeDescriptor().taskIndex, getContext().getProperties().globalDOP);

        this.inputOp.open();
    }

    @Override
    public OperatorResult<I> next() throws Throwable {

        final OperatorResult<I> input = inputOp.next();

        if (input.marker != OperatorResult.StreamMarker.END_OF_STREAM_MARKER) {
            outputFormat.writeRecord((AbstractTuple)input.element);
        }

        return input;
    }

    @Override
    public void close() throws Throwable {
        super.close();

        this.outputFormat.close();
    }

    @Override
    public void accept(final IVisitor<IPhysicalOperator> visitor) {
        visitor.visit(this);
    }
}
