package de.tuberlin.aura.demo.examples;

import java.util.Arrays;
import java.util.UUID;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.SimpleLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.RowRecordReader;
import de.tuberlin.aura.core.record.RowRecordWriter;
import de.tuberlin.aura.core.task.spi.AbstractInvokeable;
import de.tuberlin.aura.core.task.spi.IDataConsumer;
import de.tuberlin.aura.core.task.spi.IDataProducer;
import de.tuberlin.aura.core.task.spi.ITaskDriver;
import de.tuberlin.aura.core.topology.AuraGraph;


public final class RecordModelTest {

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(RecordModelTest.class);

    // Disallow Instantiation.
    private RecordModelTest() {}

    public static final class TestRecord {

        public TestRecord() {
        }

        public int a;

        public int b;

        public int c;

        public String d;
    }

    /**
     *
     */
    public static class TaskMap1 extends AbstractInvokeable {

        private final RowRecordWriter recordWriter;

        public TaskMap1(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
            super(taskDriver, producer, consumer, LOG);

            final Partitioner.IPartitioner partitioner = new Partitioner.HashPartitioner(new int[] {0});

            this.recordWriter = new RowRecordWriter(driver, TestRecord.class, 0, partitioner);
        }

        @Override
        public void run() throws Throwable {

            recordWriter.begin();

            //if(driver.getNodeDescriptor().taskIndex == 0) {
                for(int i = 0; i < 10000; ++i) {
                    final TestRecord tr = new TestRecord();
                    tr.a = i;
                    tr.b = 101;
                    tr.c = 102;
                    tr.d = "TASK 0";
                    recordWriter.writeObject(tr);
                }
            //}

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

        private final RowRecordReader recordReader;

        public TaskMap2(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {

            super(taskDriver, producer, consumer, LOG);

            this.recordReader = new RowRecordReader(taskDriver, 0);
        }

        @Override
        public void open() throws Throwable {
            consumer.openGate(0);
        }

        @Override
        public void run() throws Throwable {

            recordReader.begin();
            while (!recordReader.finished()) {
                final TestRecord obj = (TestRecord)recordReader.readObject();
                if (obj != null) {
                    String value3 = obj.d;
                    int value0 = obj.a;
                    if(driver.getNodeDescriptor().taskIndex == 0) {
                       System.out.println(driver.getNodeDescriptor().taskID + " -- value3: " + value3 + "   value0:" + value0);
                    }

                }
            }
            recordReader.end();

            /*while (!consumer.isExhausted() && isInvokeableRunning()) {
                final IOEvents.TransferBufferEvent inEvent = consumer.absorb(0);
                if (inEvent != null) {
                    inEvent.buffer.free();
                }
            }*/
        }

        @Override
        public void close() throws Throwable {
        }
    }


    // ---------------------------------------------------
    // Main.
    // ---------------------------------------------------

    public static void main(String[] args) {

        final SimpleLayout layout = new SimpleLayout();
        final ConsoleAppender consoleAppender = new ConsoleAppender(layout);

        // Local
        final String zookeeperAddress = "localhost:2181";
        final LocalClusterSimulator lce = new LocalClusterSimulator(LocalClusterSimulator.ExecutionMode.EXECUTION_MODE_SINGLE_PROCESS, true, zookeeperAddress, 4);

        final AuraClient ac = new AuraClient(zookeeperAddress, 10000, 11111);

        final AuraGraph.AuraTopologyBuilder atb1 = ac.createTopologyBuilder();

        atb1.addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "TaskMap1", 1, 1), Arrays.asList(TaskMap1.class, TestRecord.class)).
                connectTo("TaskMap2", AuraGraph.Edge.TransferType.ALL_TO_ALL)
            .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "TaskMap2", 1, 1), TaskMap2.class);





        final AuraGraph.AuraTopology at1 = atb1.build("JOB 1");

        ac.submitTopology(at1, null);
    }
}
