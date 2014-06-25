package de.tuberlin.aura.demo.examples;

import java.util.Arrays;
import java.util.UUID;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.SimpleLayout;
import org.slf4j.Logger;

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

    // Disallow Instantiation.
    private RecordModelTest() {}

    public static final class Record1 {

        public Record1() {
        }

        public int a;

        public int b;

        public int c;

        public String d;
    }

    /**
     *
     */
    public static class Source extends AbstractInvokeable {

        private final RowRecordWriter recordWriter;

        public Source(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
            super(taskDriver, producer, consumer, LOG);

            final Partitioner.IPartitioner partitioner = new Partitioner.HashPartitioner(new int[] {0});

            this.recordWriter = new RowRecordWriter(driver, Record1.class, 0, partitioner);
        }

        public void open() throws Throwable {

            recordWriter.begin();
        }


        @Override
        public void run() throws Throwable {

            for(int i = 0; i < 10000; ++i) {

                final Record1 tr = new Record1();
                tr.a = i;
                tr.b = 101;
                tr.c = 102;
                tr.d = "TASK 0";

                recordWriter.writeObject(tr);
            }
        }

        @Override
        public void close() throws Throwable {

            recordWriter.end();

            producer.done();
        }
    }

    /**
     *
     */
    public static class Sink extends AbstractInvokeable {

        private final RowRecordReader recordReader;

        public Sink(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {

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

                final Record1 obj = (Record1)recordReader.readObject();

                if (obj != null) {
                    double value0 = obj.a;
                    if(driver.getNodeDescriptor().taskIndex == 0) {
                       System.out.println(driver.getNodeDescriptor().taskID + " value0:" + value0);
                    }
                }
            }

            recordReader.end();
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
        new ConsoleAppender(layout);

        // Local
        final String zookeeperAddress = "localhost:2181";
        new LocalClusterSimulator(LocalClusterSimulator.ExecutionMode.EXECUTION_MODE_SINGLE_PROCESS, true, zookeeperAddress, 4);

        final AuraClient ac = new AuraClient(zookeeperAddress, 10000, 11111);

        final AuraGraph.AuraTopologyBuilder atb1 = ac.createTopologyBuilder();

        atb1.addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Source", 1, 1), Arrays.asList(Source.class, Record1.class))
                .connectTo("Sink", AuraGraph.Edge.TransferType.ALL_TO_ALL)
            .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Sink", 1, 1), Sink.class);

        final AuraGraph.AuraTopology at1 = atb1.build("JOB 1");

        ac.submitTopology(at1, null);
    }
}
