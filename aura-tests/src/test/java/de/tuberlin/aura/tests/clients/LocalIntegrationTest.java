package de.tuberlin.aura.tests.clients;

import java.util.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.topology.Topology;
import de.tuberlin.aura.core.task.spi.AbstractInvokeable;
import de.tuberlin.aura.core.task.spi.IDataConsumer;
import de.tuberlin.aura.core.task.spi.IDataProducer;
import de.tuberlin.aura.core.task.spi.ITaskDriver;

public class LocalIntegrationTest {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(LocalIntegrationTest.class);

    private static final String zookeeperAddress = "localhost:2181";
    private static final int machines = 2;

    private static AuraClient auraClient;


    // ---------------------------------------------------
    // Test methods.
    // ---------------------------------------------------

    @BeforeClass
    public static void setupClusterSimulatorAndClient() {
        final IConfig config = IConfigFactory.load();
        new LocalClusterSimulator(config);
        auraClient = new AuraClient(config);
    }

    @Test
    public void testMinimalTopology() {
        int executionUnits = machines * 2;

        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();
        atb.addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source", executionUnits / 2, 1), SmallSource.class)
                .connectTo("Sink", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 2, 1), Sink.class);

        runTopology(atb.build("Job: 2 layered - point2point connection"));
    }

    @Test
    public void testExtendedTopology() {
        int executionUnits = machines * 4;

        Topology.AuraTopologyBuilder atb = auraClient.createTopologyBuilder();
        atb.addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source Left", executionUnits / 6, 1), SmallSource.class)
                .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source Right", executionUnits / 6, 1), LargeSource.class)
                .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Middle", executionUnits / 6, 1), ForwardWithTwoInputs.class)
                .connectTo("Middle2", Topology.Edge.TransferType.ALL_TO_ALL)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source Middle", executionUnits / 6, 1), SmallSource.class)
                .connectTo("Middle2", Topology.Edge.TransferType.ALL_TO_ALL)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Middle2", executionUnits / 6, 1), ForwardWithTwoInputs.class)
                .connectTo("Sink", Topology.Edge.TransferType.ALL_TO_ALL)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 6, 1), Sink.class);

        runTopology(atb.build("Job: 6 layered"));
    }

    @Test
    public void testMultipleQueries() {
        int executionUnits = machines * 4;
        Topology.AuraTopologyBuilder atb;

        List<Topology.AuraTopology> topologies = new ArrayList<>();

        // 2 layered - all2all connection
        atb = auraClient.createTopologyBuilder();
        atb.addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source", executionUnits / 2, 1), LargeSource.class)
                .connectTo("Sink", Topology.Edge.TransferType.ALL_TO_ALL)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 2, 1), Sink.class);
        topologies.add(atb.build("Job: 2 layered - all2all connection"));

        // 3 layered - point2point + point2point connection
        atb = auraClient.createTopologyBuilder();
        atb.addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source", executionUnits / 3, 1), LargeSource.class)
                .connectTo("Middle", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Middle", executionUnits / 3, 1), ForwardWithOneInput.class)
                .connectTo("Sink", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 3, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - point2point + point2point connection"));

        // 3 layered - all2all + point2point connection
        atb = auraClient.createTopologyBuilder();
        atb.addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source", executionUnits / 3, 1), LargeSource.class)
                .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Middle", executionUnits / 3, 1), ForwardWithOneInput.class)
                .connectTo("Sink", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 3, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - all2all + point2point connection"));

        // 3 layered - point2point + all2all connection
        atb = auraClient.createTopologyBuilder();
        atb.addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source", executionUnits / 3, 1), LargeSource.class)
                .connectTo("Middle", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Middle", executionUnits / 3, 1), ForwardWithOneInput.class)
                .connectTo("Sink", Topology.Edge.TransferType.ALL_TO_ALL)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 3, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - point2point + all2all connection"));

        // 3 layered - all2all + all2all connection
        atb = auraClient.createTopologyBuilder();
        atb.addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source", executionUnits / 3, 1), LargeSource.class)
                .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Middle", executionUnits / 3, 1), ForwardWithOneInput.class)
                .connectTo("Sink", Topology.Edge.TransferType.ALL_TO_ALL)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 3, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - all2all + all2all connection"));

        // 3 layered - point2point (join) point2point connection
        atb = auraClient.createTopologyBuilder();
        atb.addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source Left", executionUnits / 4, 1), LargeSource.class)
                .connectTo("Middle", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source Right", executionUnits / 4, 1), LargeSource.class)
                .connectTo("Middle", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Middle", executionUnits / 4, 1), ForwardWithTwoInputs.class)
                .connectTo("Sink", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 4, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - point2point (join) point2point connection"));

        // 3 layered - all2all (join) point2point connection
        atb = auraClient.createTopologyBuilder();
        atb.addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source Left", executionUnits / 4, 1), LargeSource.class)
                .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source Right", executionUnits / 4, 1), LargeSource.class)
                .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Middle", executionUnits / 4, 1), ForwardWithTwoInputs.class)
                .connectTo("Sink", Topology.Edge.TransferType.POINT_TO_POINT)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 4, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - all2all (join) point2point connection"));

        // 3 layered - all2all (join) all2all connection
        atb = auraClient.createTopologyBuilder();
        atb.addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source Left", executionUnits / 4, 1), LargeSource.class)
                .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source Right", executionUnits / 4, 1), LargeSource.class)
                .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Middle", executionUnits / 4, 1), ForwardWithTwoInputs.class)
                .connectTo("Sink", Topology.Edge.TransferType.ALL_TO_ALL)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 4, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - all2all (join) all2all connection"));

        // 3 layered - all2all (join) all2all connection (small/large)
        atb = auraClient.createTopologyBuilder();
        atb.addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source Left", executionUnits / 4, 1), LargeSource.class)
                .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source Right", executionUnits / 4, 1), SmallSource.class)
                .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Middle", executionUnits / 4, 1), ForwardWithTwoInputs.class)
                .connectTo("Sink", Topology.Edge.TransferType.ALL_TO_ALL)
                .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 4, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - all2all (join) all2all connection (small/large)"));

        runTopologies(topologies);
    }

    @AfterClass
    public static void tearDown() {
        auraClient.closeSession();
    }

    // ---------------------------------------------------
    // Methods.
    // ---------------------------------------------------

    private static void runTopology(Topology.AuraTopology topology) {
        List<Topology.AuraTopology> topologies = Collections.singletonList(topology);

        runTopologies(topologies);
    }

    private static void runTopologies(List<Topology.AuraTopology> topologies) {
        SubmissionHandler handler = new SubmissionHandler(auraClient, topologies, 1);

        auraClient.ioManager.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED, handler);

        handler.handleTopologyFinished(null);

        auraClient.awaitSubmissionResult();
    }


    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    public static class SmallSource extends AbstractInvokeable {

        private static final int BUFFER_COUNT = 5;

        public SmallSource(final ITaskDriver driver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
            super(driver, producer, consumer, LOG);
        }

        @Override
        public void run() throws Throwable {

            int i = 0;
            while (i++ < BUFFER_COUNT && isInvokeableRunning()) {

                final MemoryView buffer = producer.getAllocator().allocBlocking();

                producer.broadcast(0, buffer);
            }

            LOG.info("Source finished");
        }

        @Override
        public void close() throws Throwable {
            LOG.debug("{} {} done", driver.getNodeDescriptor().name, driver.getNodeDescriptor().taskIndex);
            producer.done();
        }
    }

    public static class LargeSource extends AbstractInvokeable {

        private static final int BUFFER_COUNT = 10;

        public LargeSource(final ITaskDriver driver, IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
            super(driver, producer, consumer, LOG);
        }

        @Override
        public void run() throws Throwable {

            int i = 0;

            while (i++ < BUFFER_COUNT && isInvokeableRunning()) {

                final MemoryView buffer = producer.getAllocator().allocBlocking();

                producer.broadcast(0, buffer);
            }

            LOG.info("Source finished");
        }

        @Override
        public void close() throws Throwable {
            LOG.debug("{} {} done", driver.getNodeDescriptor().name, driver.getNodeDescriptor().taskIndex);
            producer.done();
        }
    }

    public static class ForwardWithOneInput extends AbstractInvokeable {

        public ForwardWithOneInput(final ITaskDriver driver, IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
            super(driver, producer, consumer, LOG);
        }

        @Override
        public void open() throws Throwable {
            consumer.openGate(0);
        }

        @Override
        public void run() throws Throwable {

            while (!consumer.isExhausted() && isInvokeableRunning()) {

                final IOEvents.TransferBufferEvent event = consumer.absorb(0);

                if (event != null) {

                    event.buffer.free();

                    final MemoryView buffer = producer.getAllocator().allocBlocking();

                    producer.broadcast(0, buffer);
                }
            }
        }

        @Override
        public void close() throws Throwable {
            LOG.debug("{} {} done", driver.getNodeDescriptor().name, driver.getNodeDescriptor().taskIndex);
            producer.done();
        }
    }

    public static class ForwardWithTwoInputs extends AbstractInvokeable {

        public ForwardWithTwoInputs(final ITaskDriver driver, IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
            super(driver, producer, consumer, LOG);
        }

        @Override
        public void open() throws Throwable {

            consumer.openGate(0);

            consumer.openGate(1);
        }

        @Override
        public void run() throws Throwable {

            while (!consumer.isExhausted() && isInvokeableRunning()) {

                final IOEvents.TransferBufferEvent leftEvent = consumer.absorb(0);

                final IOEvents.TransferBufferEvent rightEvent = consumer.absorb(1);

                if (leftEvent != null) {

                    leftEvent.buffer.free();

                    final MemoryView buffer = producer.getAllocator().allocBlocking();

                    producer.broadcast(0, buffer);
                }

                if (rightEvent != null) {

                    rightEvent.buffer.free();

                    final MemoryView buffer = producer.getAllocator().allocBlocking();

                    producer.broadcast(0, buffer);
                }
            }
        }

        @Override
        public void close() throws Throwable {
            LOG.debug("{} {} done", driver.getNodeDescriptor().name, driver.getNodeDescriptor().taskIndex);
            producer.done();
        }
    }

    public static class Sink extends AbstractInvokeable {

        long count = 0;

        public Sink(final ITaskDriver driver, IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
            super(driver, producer, consumer, LOG);
        }

        @Override
        public void open() throws Throwable {
            consumer.openGate(0);
        }

        @Override
        public void run() throws Throwable {

            while (!consumer.isExhausted() && isInvokeableRunning()) {

                final IOEvents.TransferBufferEvent event = consumer.absorb(0);

                if (event != null) {

                    count++;

                    event.buffer.free();
                }
            }

            LOG.info("Sink finished {}.", count);
        }
    }

    private static class SubmissionHandler extends EventHandler {

        private final AuraClient client;

        private final List<Topology.AuraTopology> topologies;

        private final int runs;

        private int jobCounter = 0;

        public SubmissionHandler(final AuraClient client, final List<Topology.AuraTopology> topologies, final int runs) {
            this.client = client;
            this.topologies = topologies;
            this.runs = runs;
        }

        @EventHandler.Handle(event = IOEvents.ControlIOEvent.class, type = IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED)
        private void handleTopologyFinished(final IOEvents.ControlIOEvent event) {
            if (event != null) {
                String jobName = (String) event.getPayload();
                LOG.info("Topology ({}) finished.", jobName);
            }

            if (jobCounter < runs * topologies.size()) {
                final int jobIndex = jobCounter++ / runs;

                Thread t = new Thread() {

                    public void run() {
                        // This break makes jobs easier to distinguish in the logs.
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        LOG.info("Submit: {} - run {}/{}", topologies.get(jobIndex).name, ((jobCounter - 1) % runs) + 1, runs);
                        client.submitTopology(topologies.get(jobIndex), null);
                    }
                };

                t.start();
            }
        }
    }


}
