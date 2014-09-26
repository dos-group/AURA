package de.tuberlin.aura.operators;

import java.util.ArrayList;
import java.util.List;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.dataflow.operators.PhysicalOperatorFactory;
import de.tuberlin.aura.core.dataflow.operators.base.AbstractPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.base.IOperatorEnvironment;
import de.tuberlin.aura.core.dataflow.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.dataflow.operators.impl.OperatorEnvironment;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.RowRecordReader;
import de.tuberlin.aura.core.record.RowRecordWriter;
import de.tuberlin.aura.core.record.typeinfo.GroupEndMarker;
import de.tuberlin.aura.core.taskmanager.spi.AbstractInvokeable;
import de.tuberlin.aura.core.taskmanager.spi.IDataConsumer;
import de.tuberlin.aura.core.taskmanager.spi.IRecordReader;
import de.tuberlin.aura.core.taskmanager.spi.IRecordWriter;

/**
 *
 */
public final class OperatorDriver extends AbstractInvokeable {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public static final class GateReaderOperator extends AbstractPhysicalOperator<Object> {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private static final long serialVersionUID = -1L;

        private final IRecordReader recordReader;

        private final IDataConsumer dataConsumer;

        private final int gateIndex;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public GateReaderOperator(final IOperatorEnvironment environment,
                                  final IRecordReader recordReader,
                                  final IDataConsumer dataConsumer,
                                  final int gateIndex) {
            super(environment);
            // sanity check.
            if (recordReader == null)
                throw new IllegalArgumentException("recordReader == null");
            if (dataConsumer == null)
                throw new IllegalArgumentException("dataConsumer == null");

            this.recordReader = recordReader;

            this.dataConsumer = dataConsumer;

            this.gateIndex = gateIndex;
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        @Override
        public void open() throws Throwable {
            super.open();
            dataConsumer.openGate(gateIndex);
            recordReader.begin();
        }

        @Override
        public Object next() throws Throwable {
            return recordReader.readObject();
        }

        @Override
        public void close() throws Throwable {
            super.close();
            recordReader.end();
            dataConsumer.closeGate(gateIndex);
        }

        @Override
        public void accept(IVisitor<IPhysicalOperator> visitor) {
            throw new UnsupportedOperationException();
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Descriptors.OperatorNodeDescriptor operatorNodeDescriptor;

    private AbstractPhysicalOperator<?> rootOperator;

    private final List<IRecordWriter> recordWriters;

    private final List<IRecordReader> recordReaders;

    private final List<GateReaderOperator> gateReaderOperators;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public OperatorDriver(final Descriptors.OperatorNodeDescriptor operatorNodeDescriptor) {

        this.operatorNodeDescriptor = operatorNodeDescriptor;

        this.recordReaders = new ArrayList<>();

        this.recordWriters = new ArrayList<>();

        this.gateReaderOperators = new ArrayList<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void create() throws Throwable {

        final IOperatorEnvironment environment = new OperatorEnvironment(driver.getLogger(), operatorNodeDescriptor.properties, operatorNodeDescriptor);

        if (driver.getBindingDescriptor().outputGateBindings.size() > 0) {

            final Partitioner.IPartitioner partitioner =
                    Partitioner.PartitionerFactory.createPartitioner(
                            operatorNodeDescriptor.properties.strategy,
                            operatorNodeDescriptor.properties.outputType,
                            operatorNodeDescriptor.properties.partitioningKeys
                    );

            for (int i = 0; i <  driver.getBindingDescriptor().outputGateBindings.size(); ++i) {
                final RowRecordWriter reader = new RowRecordWriter(driver, operatorNodeDescriptor.properties.outputType, i, partitioner);
                recordWriters.add(reader);
            }
        }

        for (int i = 0; i <  driver.getBindingDescriptor().inputGateBindings.size(); ++i) {
            final IRecordReader recordReader = new RowRecordReader(driver, i);
            recordReaders.add(recordReader);
            gateReaderOperators.add(new GateReaderOperator(environment, recordReader, consumer, i));
        }

        switch (operatorNodeDescriptor.properties.operatorType.operatorInputArity) {
            case NULLARY:
                rootOperator = PhysicalOperatorFactory.createPhysicalOperator(environment, null, null);
            break;
            case UNARY:
                rootOperator = PhysicalOperatorFactory.createPhysicalOperator(environment, gateReaderOperators.get(0), null);
            break;
            case BINARY:
                rootOperator = PhysicalOperatorFactory.createPhysicalOperator(environment, gateReaderOperators.get(0), gateReaderOperators.get(1));
                break;
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public void open() throws Throwable {
        for (final IRecordWriter recordWriter : recordWriters) {
            recordWriter.begin();
        }
        rootOperator.open();
    }

    @Override
    public void run() throws Throwable {

        if (operatorNodeDescriptor.properties.outputType != null &&
                operatorNodeDescriptor.properties.outputType.isGrouped()) {

            // groups: null as return value = end of a group
            //              operator closed = end of data

            // -> as this is currently only handled here in the OperatorDriver, this will be a problem as soon as
            // multiple ops are executed within the same execution unit (e.g. after compactification)

            while (rootOperator.isOpen()) {

                Object object = rootOperator.next();

                if (object != null) {
                    if(recordWriters.size() == 1) {
                        recordWriters.get(0).writeObject(object);
                    }
                } else {
                    if (rootOperator.isOpen()) {
                        if(recordWriters.size() == 1) {
                            recordWriters.get(0).writeObject(GroupEndMarker.class);
                        }
                    }
                }
            }

        } else {

            // elements: null = end of data

            Object object = rootOperator.next();

            while (object != null) {
                if(recordWriters.size() == 1) {
                    recordWriters.get(0).writeObject(object);
                }
                object = rootOperator.next();
            }
        }
    }

    @Override
    public void close() throws Throwable {
        rootOperator.close();
        for (final IRecordWriter recordWriter : recordWriters) {
            recordWriter.end();
        }
        if (driver.getBindingDescriptor().outputGateBindings.size() == 1)
            producer.done(0);
    }

    @Override
    public void release() throws Throwable {
    }
}
