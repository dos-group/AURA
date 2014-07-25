package de.tuberlin.aura.computation;

import java.util.ArrayList;
import java.util.List;

import de.tuberlin.aura.core.operators.AbstractPhysicalOperator;
import de.tuberlin.aura.core.operators.IPhysicalOperator;
import org.slf4j.Logger;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.operators.PhysicalOperatorFactory;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.RowRecordReader;
import de.tuberlin.aura.core.record.RowRecordWriter;
import de.tuberlin.aura.core.task.spi.*;

/**
 *
 */
public final class ExecutionPlanDriver extends AbstractInvokeable {

    /**
     *
     */
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

        public GateReaderOperator(final IRecordReader recordReader, final IDataConsumer dataConsumer, final int gateIndex) {
            super(null);
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
            dataConsumer.openGate(gateIndex);
            recordReader.begin();
        }

        @Override
        public Object next() throws Throwable{
            return recordReader.readObject();
        }

        @Override
        public void close() throws Throwable {
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

    private AbstractPhysicalOperator<Object> rootOperator;

    private final List<IRecordWriter> recordWriters;

    private final List<IRecordReader> recordReaders;

    private final List<GateReaderOperator> gateReaderOperators;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public ExecutionPlanDriver(final Descriptors.OperatorNodeDescriptor operatorNodeDescriptor) {

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

        if (driver.getBindingDescriptor().outputGateBindings.size() > 0) {

            final Partitioner.IPartitioner partitioner =
                    Partitioner.PartitionerFactory.createPartitioner(
                            operatorNodeDescriptor.properties.strategy,
                            operatorNodeDescriptor.properties.keys
                    );

            for (int i = 0; i <  driver.getBindingDescriptor().outputGateBindings.size(); ++i) {
                recordWriters.add(new RowRecordWriter(driver, operatorNodeDescriptor.properties.outputType, i, partitioner));
            }
        }

        for (int i = 0; i <  driver.getBindingDescriptor().inputGateBindings.size(); ++i) {

            final IRecordReader recordReader = new RowRecordReader(driver, i);

            recordReaders.add(recordReader);

            gateReaderOperators.add(new GateReaderOperator(recordReader, consumer, i));
        }

        switch (operatorNodeDescriptor.properties.operatorType.operatorInputArity) {

            case NULLARY:
                rootOperator = PhysicalOperatorFactory.createPhysicalOperator(operatorNodeDescriptor.properties);
            break;

            case UNARY:
                rootOperator = PhysicalOperatorFactory.createPhysicalOperator(operatorNodeDescriptor.properties, gateReaderOperators.get(0));
            break;

            case BINARY:
                rootOperator = PhysicalOperatorFactory.createPhysicalOperator(operatorNodeDescriptor.properties, gateReaderOperators.get(0), gateReaderOperators.get(1));
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

        switch (rootOperator.getProperties().operatorType) {

            case MAP_TUPLE_OPERATOR: {

                Object object = rootOperator.next();
                while (object != null) {
                    emit(object);
                    object = rootOperator.next();
                }

            } break;
            case MAP_GROUP_OPERATOR: {
            } break;
            case FLAT_MAP_TUPLE_OPERATOR:
                break;
            case FLAT_MAP_GROUP_OPERATOR:
                break;
            case FILTER_OPERATOR:
                break;
            case UNION_OPERATOR:
                break;
            case DIFFERENCE_OPERATOR:
                break;
            case HASH_JOIN_OPERATOR:
                break;
            case MERGE_JOIN_OPERATOR:
                break;
            case GROUP_BY_OPERATOR:
                break;
            case SORT_OPERATOR:
                break;
            case REDUCE_OPERATOR:
                break;

            case UDF_SOURCE: {

                Object object = rootOperator.next();
                while (object != null) {
                    emit(object);
                    object = rootOperator.next();
                }

            } break;

            case FILE_SOURCE: {
            } break;

            case STREAM_SOURCE:
                break;

            case UDF_SINK: {

                while (!recordReaders.get(0).finished()) {
                    rootOperator.next();
                }

            } break;

            case FILE_SINK: {
            } break;
            case STREAM_SINK:
                break;
        }
    }

    @Override
    public void close() throws Throwable {

        rootOperator.close();

        for (final IRecordWriter recordWriter : recordWriters) {
            recordWriter.end();
        }

        if (driver.getBindingDescriptor().outputGateBindings.size() > 0)
            producer.done(0);
    }

    @Override
    public void release() throws Throwable {
    }

    private void emit(final Object object) {
        if (object != null) {
            recordWriters.get(0).writeObject(object);
        }
    }
}
