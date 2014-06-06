package de.tuberlin.aura.demo.examples;

import java.util.UUID;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.SimpleLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.record.RowRecordModel;
import de.tuberlin.aura.core.task.spi.AbstractInvokeable;
import de.tuberlin.aura.core.task.spi.IDataConsumer;
import de.tuberlin.aura.core.task.spi.IDataProducer;
import de.tuberlin.aura.core.task.spi.ITaskDriver;
import de.tuberlin.aura.core.topology.AuraGraph;
import de.tuberlin.aura.taskmanager.TaskRecordReader;
import de.tuberlin.aura.taskmanager.TaskRecordWriter;


public final class SimpleClient2 {

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(SimpleClient2.class);

    // Disallow Instantiation.
    private SimpleClient2() {}

    /**
     *
     */
    public static class TaskMap1 extends AbstractInvokeable {

        private final TaskRecordWriter recordWriter;

        private final Class<?> recordType;

        public TaskMap1(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
            super(taskDriver, producer, consumer, LOG);

            this.recordType = RowRecordModel.RecordTypeBuilder.buildRecordType(new Class<?>[] {int.class, int.class, int.class});

            final RowRecordModel.Partitioner partitioner = new RowRecordModel.HashPartitioner(new int[] {1});

            this.recordWriter = new TaskRecordWriter(driver, recordType, partitioner);
        }

        @Override
        public void run() throws Throwable {

            recordWriter.begin();

            for(int i = 0; i < 180000; ++i) {

                final RowRecordModel.Record record = RowRecordModel.RecordTypeBuilder.createRecord(recordType);
                record.setInt(0, i);
                record.setInt(1, 101);
                record.setInt(2, 101);

                recordWriter.writeRecord(record);
            }

            recordWriter.end();
        }

        @Override
        public void close() throws Throwable {
            producer.done();
        }
    }

    /**
     *
     */
    public static class TaskMap2 extends AbstractInvokeable {

        private TaskRecordReader recordReader;

        private final Class<?> recordType;

        public TaskMap2(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {

            super(taskDriver, producer, consumer, LOG);

            this.recordType = RowRecordModel.RecordTypeBuilder.buildRecordType(new Class<?>[] {int.class, int.class, int.class});

            this.recordReader = new TaskRecordReader(taskDriver, 0, recordType);
        }

        @Override
        public void open() throws Throwable {
            consumer.openGate(0);
        }

        @Override
        public void run() throws Throwable {

            while (!recordReader.isReaderFinished() && !consumer.isExhausted()) {

                final RowRecordModel.Record record = recordReader.readRecord();

                if (record != null) {
                    int value = record.getInt(0);
                    //System.out.println(value);
                }
            }

            recordReader.close();
        }
    }

    // ---------------------------------------------------
    // Main.
    // ---------------------------------------------------

    public static void main(String[] args) {

        final SimpleLayout layout = new SimpleLayout();
        final ConsoleAppender consoleAppender = new ConsoleAppender(layout);

        // Local
        final String measurementPath = "/home/tobias/Desktop/logs";
        final String zookeeperAddress = "localhost:2181";
        final LocalClusterSimulator lce = new LocalClusterSimulator(LocalClusterSimulator.ExecutionMode.EXECUTION_MODE_SINGLE_PROCESS, true, zookeeperAddress, 4, measurementPath);

        final AuraClient ac = new AuraClient(zookeeperAddress, 10000, 11111);

        final AuraGraph.AuraTopologyBuilder atb1 = ac.createTopologyBuilder();

        atb1.addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "TaskMap1", 1, 1), TaskMap1.class).
                connectTo("TaskMap2", AuraGraph.Edge.TransferType.POINT_TO_POINT)
            .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "TaskMap2", 1, 1), TaskMap2.class);

        final AuraGraph.AuraTopology at1 = atb1.build("JOB 1");

        ac.submitTopology(at1, null);
    }
}
