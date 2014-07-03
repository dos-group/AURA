package de.tuberlin.aura.computation;

import de.tuberlin.aura.core.common.utils.Visitor;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.operators.PhysicalOperatorFactory;
import de.tuberlin.aura.core.operators.PhysicalOperators;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.RowRecordReader;
import de.tuberlin.aura.core.record.RowRecordWriter;
import de.tuberlin.aura.core.task.spi.*;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public final class ExecutionPlanDriver extends AbstractInvokeable {

    public static final class GateReaderOperator extends PhysicalOperators.AbstractPhysicalOperator<Object> {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private static final long serialVersionUID = -1L;

        private final IRecordReader recordReader;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public GateReaderOperator(final IRecordReader recordReader) {
            super(null);
            // sanity check.
            if (recordReader == null)
                throw new IllegalArgumentException("recordReader == null");

            this.recordReader = recordReader;
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        @Override
        public Object next() {
            return recordReader.readObject();
        }

        @Override
        public void accept(Visitor<PhysicalOperators.IPhysicalOperator> visitor) {
            throw new UnsupportedOperationException();
        }
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Descriptors.OperatorNodeDescriptor operatorNodeDescriptor;

    private PhysicalOperators.AbstractPhysicalOperator<Object> rootOperator;

    private final List<IRecordWriter> recordWriters;

    private final List<IRecordReader> recordReaders;

    private final List<GateReaderOperator> gateReaderOperators;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public ExecutionPlanDriver(final ITaskDriver driver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG,
                               final Descriptors.OperatorNodeDescriptor operatorNodeDescriptor) {

        super(driver, producer, consumer, LOG);

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

            gateReaderOperators.add(new GateReaderOperator(recordReader));
        }

        switch (operatorNodeDescriptor.properties.operatorType.operatorInputArity) {

            case NULLARY:
                rootOperator = PhysicalOperatorFactory.createPhysicalOperator(operatorNodeDescriptor.properties, null, null);
            break;

            case UNARY:
                rootOperator = PhysicalOperatorFactory.createPhysicalOperator(operatorNodeDescriptor.properties, gateReaderOperators.get(0), null);
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

        for (int i = 0; i <  driver.getBindingDescriptor().inputGateBindings.size(); ++i)
            consumer.openGate(i);

        for (final IRecordReader recordReader : recordReaders) {
            recordReader.begin();
        }

        for (final IRecordWriter recordWriter : recordWriters) {
            recordWriter.begin();
        }

        rootOperator.open();
    }

    @Override
    public void run() throws Throwable {

        switch (rootOperator.getProperties().operatorType) {

            case MAP_TUPLE_OPERATOR:

                while (!recordReaders.get(0).finished()) {

                    final Object object = rootOperator.next();

                    if (object != null) {
                        recordWriters.get(0).writeObject(object);
                    }
                }

                break;
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

                Object object;

                do {

                    object = rootOperator.next();

                    if (object != null) {
                        recordWriters.get(0).writeObject(object);
                    }

                } while (object != null);

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

        for (final IRecordReader recordReader : recordReaders) {
            recordReader.end();
        }

        for (final IRecordWriter recordWriter : recordWriters) {
            recordWriter.end();
        }

        if (driver.getBindingDescriptor().outputGateBindings.size() > 0)
            producer.done();
    }

    @Override
    public void release() throws Throwable {
    }
}
