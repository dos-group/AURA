package de.tuberlin.aura.drivers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.dataflow.operators.PhysicalOperatorFactory;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.impl.ExecutionContext;
import de.tuberlin.aura.core.record.*;
import de.tuberlin.aura.core.taskmanager.spi.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import static de.tuberlin.aura.core.record.OperatorResult.StreamMarker;


public final class OperatorDriver extends AbstractInvokeable {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public static final class GateReaderOperator extends AbstractPhysicalOperator<Object> {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private static final long serialVersionUID = -1L;

        private final IRecordReader reader;

        private final IDataConsumer consumer;

        private final int gateIndex;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public GateReaderOperator(final IExecutionContext environment,
                                  final IRecordReader reader,
                                  final IDataConsumer consumer,
                                  final int gateIndex) {

            super(environment);

            // sanity check.
            if (reader == null)
                throw new IllegalArgumentException("reader == null");
            if (consumer == null)
                throw new IllegalArgumentException("consumer == null");

            this.reader = reader;

            this.consumer = consumer;

            this.gateIndex = gateIndex;
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        @Override
        public void open() throws Throwable {
            super.open();
            consumer.openGate(gateIndex);
        }

        @Override
        public OperatorResult<Object> next() throws Throwable {

            Object input = reader.readObject();

            return new OperatorResult<>(input, this.markerForGateInput(input));
        }

        @Override
        public void close() throws Throwable {
            super.close();
            consumer.closeGate(gateIndex);
        }

        @Override
        public void accept(IVisitor<IPhysicalOperator> visitor) {
            throw new UnsupportedOperationException();
        }

        private StreamMarker markerForGateInput(Object input) {

            if (input == null) {
                return StreamMarker.END_OF_STREAM_MARKER;
            } else if (input instanceof RowRecordModel.RECORD_CLASS_GROUP_END) {
                return StreamMarker.END_OF_GROUP_MARKER;
            } else {
                return null;
            }
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Descriptors.OperatorNodeDescriptor nodeDescriptor;

    private AbstractPhysicalOperator<?> operator;

    private final List<IRecordWriter> writers;

    private final List<IRecordReader> readers;

    private final List<AbstractPhysicalOperator<Object>> gateReaders;

    private final IExecutionContext context;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public OperatorDriver(final ITaskRuntime runtime,
                          final Descriptors.OperatorNodeDescriptor nodeDescriptor,
                          final Descriptors.NodeBindingDescriptor bindingDescriptor) {

        this.nodeDescriptor = nodeDescriptor;

        this.writers = new ArrayList<>();

        this.readers = new ArrayList<>();

        this.gateReaders = new ArrayList<>();

        this.context = new ExecutionContext(runtime, nodeDescriptor, bindingDescriptor);

        final Configuration conf = new Configuration();
        conf.set("fs.defaultFS", getExecutionContext().getRuntime().getTaskManager().getConfig().getString("tm.io.hdfs.hdfs_url"));
        context.put("hdfs_config", conf);

        for (final DataflowNodeProperties properties : nodeDescriptor.propertiesList) {
            if (properties.config != null) {
                // TODO: check for overrides...
                for (final Map.Entry<String, Object> entry : properties.config.entrySet())
                    context.put(entry.getKey(), entry.getValue());
            }
        }
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void create() throws Throwable {

        if (runtime.getBindingDescriptor().outputGateBindings.size() > 0) {

            int lastOperatorNum = nodeDescriptor.propertiesList.size() - 1;

            final Partitioner.IPartitioner partitioner = (nodeDescriptor.propertiesList.get(lastOperatorNum).strategy != null) ?
                    Partitioner.PartitionerFactory.createPartitioner(
                            nodeDescriptor.propertiesList.get(lastOperatorNum).strategy,
                            nodeDescriptor.propertiesList.get(lastOperatorNum).outputType,
                            nodeDescriptor.propertiesList.get(lastOperatorNum).partitionKeyIndices
                    ) : null;

            for (int i = 0; i <  runtime.getBindingDescriptor().outputGateBindings.size(); ++i) {
                final RecordWriter writer = new RecordWriter(runtime, nodeDescriptor.propertiesList.get(lastOperatorNum).outputType, i, partitioner);
                writers.add(writer);
            }
        }

        for (int i = 0; i <  runtime.getBindingDescriptor().inputGateBindings.size(); ++i) {
            final IRecordReader reader = new RecordReader(runtime, i);
            readers.add(reader);
            gateReaders.add(new GateReaderOperator(context, reader, consumer, i));
        }

        operator = PhysicalOperatorFactory.createPhysicalOperatorPlan(context, gateReaders);

        for (final IRecordWriter writer : writers)
            writer.begin();

        for (final DataflowNodeProperties properties : nodeDescriptor.propertiesList) {
            if (properties.broadcastVars != null) {
                for (final UUID datasetID : properties.broadcastVars)
                    context.putDataset(datasetID, runtime.getTaskManager().getBroadcastDataset(datasetID));
            }
        }

        operator.open();
    }

    @Override
    public void open() throws Throwable {

        for (final IRecordReader reader : readers)
            reader.begin();

        for (final IRecordWriter writer : writers)
            writer.begin();
    }

    @Override
    public void run() throws Throwable {

        OperatorResult<?> input = operator.next();

        while (input.marker != StreamMarker.END_OF_STREAM_MARKER) {

            Object element = (input.marker == StreamMarker.END_OF_GROUP_MARKER) ?
                    new RowRecordModel.RECORD_CLASS_GROUP_END() : input.element;

            for (int gateIndex : operator.getOutputGates()) {
                writers.get(gateIndex).writeObject(element);
            }

            input = operator.next();
        }
    }

    @Override
    public void close() throws Throwable {

        for (final IRecordReader reader : readers)
            reader.end();

        for (final IRecordWriter writer : writers)
            writer.end();
    }

    @Override
    public void release() throws Throwable {

        for (int i = 0; i <  runtime.getBindingDescriptor().outputGateBindings.size(); ++i)
            producer.done(i);

        //Thread.sleep(1000);

        //operator.close();
    }

    public IExecutionContext getExecutionContext() {
        return context;
    }
}
