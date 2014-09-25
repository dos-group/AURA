package de.tuberlin.aura.demo.examples;

import java.util.UUID;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.SimpleLayout;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.TypeInformation;
import de.tuberlin.aura.core.record.RowRecordReader;
import de.tuberlin.aura.core.record.RowRecordWriter;
import de.tuberlin.aura.core.record.tuples.AbstractTuple;
import de.tuberlin.aura.core.record.tuples.Tuple3;
import de.tuberlin.aura.core.taskmanager.spi.AbstractInvokeable;
import de.tuberlin.aura.core.topology.Topology;


public final class RecordModelTest {

    // Disallow Instantiation.
    private RecordModelTest() {}

    public static final class Record1 {

        public Record1() {}

        public int a;

        public int b;

        public int c;

        public String d;
    }

    /**
     *
     */
    public static class Source extends AbstractInvokeable {

        private RowRecordWriter recordWriter;

        public Source() {
        }

        public void open() throws Throwable {

            final TypeInformation outputTypeInfo =
                    new TypeInformation(Tuple3.class,
                            new TypeInformation(String.class),
                            new TypeInformation(Integer.class),
                            new TypeInformation(Integer.class));

            final Partitioner.IPartitioner partitioner = new Partitioner.HashPartitioner(outputTypeInfo, new int[][] { {0} });
            this.recordWriter = new RowRecordWriter(driver, AbstractTuple.class, 0, partitioner);
            recordWriter.begin();
        }


        @Override
        public void run() throws Throwable {
            for (int i = 0; i < 10000; ++i) {
                recordWriter.writeObject(new Tuple3<>("Hans" + i, i, i));
            }
        }

        @Override
        public void close() throws Throwable {
            recordWriter.end();
            producer.done(0);
        }
    }

    /**
     *
     */
    public static class Sink extends AbstractInvokeable {

        private RowRecordReader recordReader;

        public Sink() {
        }

        @Override
        public void open() throws Throwable {
            this.recordReader = new RowRecordReader(driver, 0);
            consumer.openGate(0);
        }

        @Override
        public void run() throws Throwable {
            recordReader.begin();
            while (!recordReader.finished()) {
                final Tuple3<String, Integer, Integer> t = (Tuple3) recordReader.readObject();
                if (t != null) {
                    double value0 = t._2;
                    if (driver.getNodeDescriptor().taskIndex == 0) {
                        System.out.println(driver.getNodeDescriptor().taskID + " value0:" + value0);
                    }
                }
            }
            recordReader.end();
        }

        @Override
        public void close() throws Throwable {}
    }

    // ---------------------------------------------------
    // Main.
    // ---------------------------------------------------

    public static void main(String[] args) {

        final SimpleLayout layout = new SimpleLayout();
        new ConsoleAppender(layout);

        final LocalClusterSimulator lcs = new LocalClusterSimulator(IConfigFactory.load(IConfig.Type.SIMULATOR));
        final AuraClient ac = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT));
        final Topology.AuraTopologyBuilder atb1 = ac.createTopologyBuilder();

        //@formatter:off
        atb1.addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Source", 1, 1), Source.class, Record1.class)
            .connectTo("Sink", Topology.Edge.TransferType.ALL_TO_ALL)
            .addNode(new Topology.InvokeableNode(UUID.randomUUID(), "Sink", 1, 1), Sink.class);
        //@formatter:on

        final Topology.AuraTopology at1 = atb1.build("JOB 1");
        ac.submitTopology(at1, null);
        ac.awaitSubmissionResult(1);
        ac.closeSession();
        lcs.shutdown();
    }
}
