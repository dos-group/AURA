package de.tuberlin.aura.demo.examples;

import java.util.Arrays;
import java.util.UUID;

import de.tuberlin.aura.core.record.tuples.AbstractTuple;
import de.tuberlin.aura.core.record.tuples.Tuple3;
import de.tuberlin.aura.core.topology.Topology;
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

            this.recordWriter = new RowRecordWriter(driver, AbstractTuple.class, 0, partitioner);
        }

        public void open() throws Throwable {

            recordWriter.begin();
        }


        @Override
        public void run() throws Throwable {

            for(int i = 0; i < 10000; ++i) {

                recordWriter.writeObject(new Tuple3<>("Hans" + i, i, i));
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

                //final Record1 obj = (Record1)recordReader.readObject();

                final Tuple3<String,Integer,Integer> t = (Tuple3)recordReader.readObject();

                if (t != null) {
                    double value0 = t._2;
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

        final Topology.AuraTopologyBuilder atb1 = ac.createTopologyBuilder();

        atb1.addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source", 1, 1), Arrays.asList(Source.class, Record1.class))
                .connectTo("Sink", Topology.Edge.TransferType.ALL_TO_ALL)
            .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Sink", 1, 1), Sink.class);

        final Topology.AuraTopology at1 = atb1.build("JOB 1");

        ac.submitTopology(at1, null);
    }
}
