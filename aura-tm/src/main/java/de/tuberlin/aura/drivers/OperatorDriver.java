package de.tuberlin.aura.drivers;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.dataflow.operators.PhysicalOperatorFactory;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IExecutionContext;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.impl.ExecutionContext;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.RecordReader;
import de.tuberlin.aura.core.record.RecordWriter;
import de.tuberlin.aura.core.record.typeinfo.GroupEndMarker;
import de.tuberlin.aura.core.taskmanager.spi.AbstractInvokeable;
import de.tuberlin.aura.core.taskmanager.spi.IDataConsumer;
import de.tuberlin.aura.core.taskmanager.spi.IRecordReader;
import de.tuberlin.aura.core.taskmanager.spi.IRecordWriter;


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
            reader.begin();
        }

        @Override
        public Object next() throws Throwable {
            return reader.readObject();
        }

        @Override
        public void close() throws Throwable {
            super.close();
            reader.end();
            consumer.closeGate(gateIndex);
        }

        @Override
        public void accept(IVisitor<IPhysicalOperator> visitor) {
            throw new UnsupportedOperationException();
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Descriptors.OperatorNodeDescriptor nodeDescriptor;

    private AbstractPhysicalOperator<?> operator;

    private final List<IRecordWriter> writers;

    private final List<AbstractPhysicalOperator<Object>> gateReaders;

    private final IExecutionContext context;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public OperatorDriver(final Descriptors.OperatorNodeDescriptor nodeDescriptor,
                          final Descriptors.NodeBindingDescriptor bindingDescriptor) {

        this.nodeDescriptor = nodeDescriptor;

        this.writers = new ArrayList<>();

        this.gateReaders = new ArrayList<>();

        this.context = new ExecutionContext(nodeDescriptor, bindingDescriptor);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void create() throws Throwable {

        if (runtime.getBindingDescriptor().outputGateBindings.size() > 0) {
            final Partitioner.IPartitioner partitioner =
                    Partitioner.PartitionerFactory.createPartitioner(
                            nodeDescriptor.properties.strategy,
                            nodeDescriptor.properties.outputType,
                            nodeDescriptor.properties.partitionKeyIndices
                    );

            for (int i = 0; i <  runtime.getBindingDescriptor().outputGateBindings.size(); ++i) {
                final RecordWriter reader = new RecordWriter(runtime, nodeDescriptor.properties.outputType, i, partitioner);
                writers.add(reader);
            }
        }

        for (int i = 0; i <  runtime.getBindingDescriptor().inputGateBindings.size(); ++i) {
            final IRecordReader recordReader = new RecordReader(runtime, i);
            gateReaders.add(new GateReaderOperator(context, recordReader, consumer, i));
        }

        operator = PhysicalOperatorFactory.createPhysicalOperator(context, gateReaders);
    }

    @Override
    public void open() throws Throwable {

        for (final IRecordWriter writer : writers)
            writer.begin();

        if (nodeDescriptor.properties.broadcastVars != null) {
            for(final UUID datasetID : nodeDescriptor.properties.broadcastVars)
                context.putDataset(datasetID, runtime.getTaskManager().getBroadcastDataset(datasetID));
        }

        operator.open();
    }

    @Override
    public void run() throws Throwable {

        if (nodeDescriptor.properties.outputType != null &&
                nodeDescriptor.properties.outputType.isGrouped()) {

            // groups: null as return value = end of a group
            //              operator closed = end of data

            // -> as this is currently only handled here in the OperatorDriver, this will be a problem as soon as
            // multiple ops are executed within the same execution unit (e.g. after compactification)

            while (operator.isOpen()) {

                Object object = operator.next();

                if (object != null) {

                    for (int gateIndex : operator.getOutputGates())
                        writers.get(gateIndex).writeObject(object);

                } else {

                    if (operator.isOpen()) {
                        for (int gateIndex : operator.getOutputGates())
                            writers.get(gateIndex).writeObject(GroupEndMarker.class);
                    }
                }
            }

        } else {

            // elements: null = end of data

            Object object = operator.next();

            while (object != null) {

                for (int gateIndex : operator.getOutputGates())
                    writers.get(gateIndex).writeObject(object);

                object = operator.next();
            }
        }
    }

    @Override
    public void close() throws Throwable {
        operator.close();

        for (final IRecordWriter writer : writers)
            writer.end();

        for (int i = 0; i <  runtime.getBindingDescriptor().outputGateBindings.size(); ++i)
            producer.done(i);
    }

    @Override
    public void release() throws Throwable {
    }

    public IExecutionContext getExecutionContext() {
        return context;
    }
}
