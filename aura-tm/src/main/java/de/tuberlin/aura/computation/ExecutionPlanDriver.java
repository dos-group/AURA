package de.tuberlin.aura.computation;

import de.tuberlin.aura.core.operators.Operators;
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

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private Operators.AbstractOperator rootOperator;

    private final List<IRecordWriter> recordWriters;

    private final List<IRecordReader> recordReaders;

    private final List<Operators.IOperator> gateReaderOperators;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public ExecutionPlanDriver(final ITaskDriver driver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
        super(driver, producer, consumer, LOG);

        this.recordReaders = new ArrayList<>();

        this.recordWriters = new ArrayList<>();

        this.gateReaderOperators = new ArrayList<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void setOperator(final Operators.IOperator operator) {
        // sanity check.
        if (operator == null)
            throw new IllegalArgumentException("operator == null");

        this.rootOperator = (Operators.AbstractOperator)operator;
    }

    @Override
    public void create() throws Throwable {

        final Partitioner.IPartitioner partitioner =
                Partitioner.PartitionerFactory.createPartitioner(
                        rootOperator.getOperatorDescriptor().strategy,
                        rootOperator.getOperatorDescriptor().keys
                );

        for (int i = 0; i <  driver.getBindingDescriptor().outputGateBindings.size(); ++i) {
            recordWriters.add(new RowRecordWriter(driver, rootOperator.getOperatorDescriptor().getUserCodeClasses().get(2), i, partitioner));
        }

        for (int i = 0; i <  driver.getBindingDescriptor().inputGateBindings.size(); ++i) {

            final IRecordReader recordReader = new RowRecordReader(driver, i);

            recordReaders.add(recordReader);

            gateReaderOperators.add(new Operators.GateReaderOperator(recordReader));
        }
    }

    @Override
    public void open() throws Throwable {

        consumer.openGate(0);

        for (final IRecordReader recordReader : recordReaders) {
            recordReader.begin();
        }

        for (final IRecordWriter recordWriter : recordWriters) {
            recordWriter.begin();
        }

        rootOperator.addChild(gateReaderOperators.get(0));

        rootOperator.open();
    }

    @Override
    public void run() throws Throwable {

        while (!recordReaders.get(0).finished()) {

            final Object object = rootOperator.next();

            if (object != null) {
                recordWriters.get(0).writeObject(object);
            }
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

        producer.done();
    }

    @Override
    public void release() throws Throwable {
    }
}
