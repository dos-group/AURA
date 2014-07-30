package de.tuberlin.aura.computation;

import java.util.ArrayList;
import java.util.List;

import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.processing.operators.PhysicalOperatorFactory;
import de.tuberlin.aura.core.processing.operators.base.AbstractPhysicalOperator;
import de.tuberlin.aura.core.processing.operators.base.IOperatorEnvironment;
import de.tuberlin.aura.core.processing.operators.base.IPhysicalOperator;
import de.tuberlin.aura.core.processing.operators.impl.OperatorEnvironment;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.RowRecordReader;
import de.tuberlin.aura.core.record.RowRecordWriter;
import de.tuberlin.aura.core.task.spi.AbstractInvokeable;
import de.tuberlin.aura.core.task.spi.IDataConsumer;
import de.tuberlin.aura.core.task.spi.IRecordReader;
import de.tuberlin.aura.core.task.spi.IRecordWriter;

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
        public Object next() throws Throwable{
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

    private IOperatorEnvironment environment;

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

        this.environment = new OperatorEnvironment(driver.getLogger(), operatorNodeDescriptor.properties);

        if (driver.getBindingDescriptor().outputGateBindings.size() > 0) {

            final Partitioner.IPartitioner partitioner =
                    Partitioner.PartitionerFactory.createPartitioner(
                            operatorNodeDescriptor.properties.strategy,
                            operatorNodeDescriptor.properties.outputType,
                            operatorNodeDescriptor.properties.partitioningKeys
                    );

            for (int i = 0; i <  driver.getBindingDescriptor().outputGateBindings.size(); ++i) {
                final RowRecordWriter reader = new RowRecordWriter(driver, operatorNodeDescriptor.properties.outputType.type, i, partitioner);
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

        /*switch (rootOperator.getProperties().operatorType) {
            case MAP_TUPLE_OPERATOR:
            case MAP_GROUP_OPERATOR:
            case FLAT_MAP_TUPLE_OPERATOR:
            case FLAT_MAP_GROUP_OPERATOR:
            case FILTER_OPERATOR:
            case UNION_OPERATOR:
            case DIFFERENCE_OPERATOR:
            case HASH_JOIN_OPERATOR:
            case MERGE_JOIN_OPERATOR:
            case GROUP_BY_OPERATOR:
            case SORT_OPERATOR:
            case REDUCE_OPERATOR:
            case FILE_SOURCE:
            case STREAM_SOURCE:
            case UDF_SOURCE:
            case FILE_SINK:
            case STREAM_SINK:
            case UDF_SINK: {
            } break;
        }*/

        Object object = rootOperator.next();
        while (object != null) {
            if(recordWriters.size() == 1) {
                recordWriters.get(0).writeObject(object);
            }
            object = rootOperator.next();
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
}
