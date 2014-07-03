package de.tuberlin.aura.demo.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.task.spi.AbstractInvokeable;
import de.tuberlin.aura.core.task.spi.IDataConsumer;
import de.tuberlin.aura.core.task.spi.IDataProducer;
import de.tuberlin.aura.core.task.spi.ITaskDriver;
import de.tuberlin.aura.core.topology.Topology;

public final class IntegrationTests {

    /**
     * Logger.
     */
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LazyUnrollingTest.class);

    // Disallow Instantiation.
    private IntegrationTests() {}

    /**
     *
     */
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

    /**
     *
     */
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

    /**
     *
     */
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

    /**
     *
     */
    public static class SinkWithTwoInputs extends AbstractInvokeable {

        long count = 0;

        public SinkWithTwoInputs(final ITaskDriver driver, IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
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

                final IOEvents.TransferBufferEvent left = consumer.absorb(0);

                final IOEvents.TransferBufferEvent right = consumer.absorb(1);

                if (left != null) {
                    count++;
                    // if (count % 10000 == 0)
                    // LOG.info("Sink leftInput receive {}.", count);
                    // LOG.info("free in sink");
                    left.buffer.free();
                }
                if (right != null) {
                    count++;
                    // if (count % 10000 == 0)
                    // LOG.info("Sink rightInput receive {}.", count);
                    // LOG.info("free in sink");
                    right.buffer.free();
                }
            }

            LOG.info("Sink finished {}.", count);
        }
    }

    /**
     * 
     * @param client
     * @param machines
     * @param tasksPerMachine
     * @return
     */
    public static List<Topology.AuraTopology> buildTopologies(AuraClient client, int machines, int tasksPerMachine) {

        List<Topology.AuraTopology> topologies = new ArrayList<>();

        int executionUnits = machines * tasksPerMachine;

        Topology.AuraTopologyBuilder atb;

        // @formatter:off

        // // 2 layered - point2point connection
        atb = client.createTopologyBuilder();
        atb.addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source", 1, 1), LargeSource.class)
           .connectTo("Sink", Topology.Edge.TransferType.POINT_TO_POINT)
           .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Sink", 1, 1), Sink.class);
        topologies.add(atb.build("Job: 2 layered - point2point connection"));

        // 2 layered - all2all connection
        atb = client.createTopologyBuilder();
        atb.addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source", executionUnits / 2, 1), LargeSource.class)
           .connectTo("Sink", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 2, 1), Sink.class);
        topologies.add(atb.build("Job: 2 layered - all2all connection"));

        // 3 layered - point2point + point2point connection
        atb = client.createTopologyBuilder();
        atb.addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source", executionUnits / 3, 1), LargeSource.class)
           .connectTo("Middle", Topology.Edge.TransferType.POINT_TO_POINT)
           .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Middle", executionUnits / 3, 1), ForwardWithOneInput.class)
           .connectTo("Sink", Topology.Edge.TransferType.POINT_TO_POINT)
           .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 3, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - point2point + point2point connection"));

        // 3 layered - all2all + point2point connection
        atb = client.createTopologyBuilder();
        atb.addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source", executionUnits / 3, 1), LargeSource.class)
           .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Middle", executionUnits / 3, 1), ForwardWithOneInput.class)
           .connectTo("Sink", Topology.Edge.TransferType.POINT_TO_POINT)
           .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 3, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - all2all + point2point connection"));

        // 3 layered - point2point + all2all connection
        atb = client.createTopologyBuilder();
        atb.addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source", executionUnits / 3, 1), LargeSource.class)
           .connectTo("Middle", Topology.Edge.TransferType.POINT_TO_POINT)
           .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Middle", executionUnits / 3, 1), ForwardWithOneInput.class)
           .connectTo("Sink", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 3, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - point2point + all2all connection"));

        // 3 layered - all2all + all2all connection
        atb = client.createTopologyBuilder();
        atb.addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source", executionUnits / 3, 1), LargeSource.class)
           .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Middle", executionUnits / 3, 1), ForwardWithOneInput.class)
           .connectTo("Sink", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 3, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - all2all + all2all connection"));

        // 3 layered - point2point (join) point2point connection
        atb = client.createTopologyBuilder();
        atb.addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source Left", executionUnits / 4, 1), LargeSource.class)
           .connectTo("Middle", Topology.Edge.TransferType.POINT_TO_POINT)
           .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source Right", executionUnits / 4, 1), LargeSource.class)
           .connectTo("Middle", Topology.Edge.TransferType.POINT_TO_POINT)
           .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Middle", executionUnits / 4, 1), ForwardWithTwoInputs.class)
           .connectTo("Sink", Topology.Edge.TransferType.POINT_TO_POINT)
           .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 4, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - point2point (join) point2point connection"));

        // 3 layered - all2all (join) point2point connection
        atb = client.createTopologyBuilder();
        atb.addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source Left", executionUnits / 4, 1), LargeSource.class)
           .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source Right", executionUnits / 4, 1), LargeSource.class)
           .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Middle", executionUnits / 4, 1), ForwardWithTwoInputs.class)
           .connectTo("Sink", Topology.Edge.TransferType.POINT_TO_POINT)
           .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 4, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - all2all (join) point2point connection"));

        // 3 layered - all2all (join) all2all connection
        atb = client.createTopologyBuilder();
        atb.addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source Left", executionUnits / 4, 1), LargeSource.class)
           .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source Right", executionUnits / 4, 1), LargeSource.class)
           .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Middle", executionUnits / 4, 1), ForwardWithTwoInputs.class)
           .connectTo("Sink", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 4, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - all2all (join) all2all connection"));

        // 3 layered - all2all (join) all2all connection (small/large)
        atb = client.createTopologyBuilder();
        atb.addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source Left", executionUnits / 4, 1), LargeSource.class)
           .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Source Right", executionUnits / 4, 1), SmallSource.class)
           .connectTo("Middle", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Middle", executionUnits / 4, 1), ForwardWithTwoInputs.class)
           .connectTo("Sink", Topology.Edge.TransferType.ALL_TO_ALL)
           .addNode(new Topology.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 4, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - all2all (join) all2all connection (small/large)"));

        // 6 layered
        atb = client.createTopologyBuilder();
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
        topologies.add(atb.build("Job: 6 layered"));

        // @formatter:on

        return topologies;
    }

    /**
     *
     */
    private static class SubmissionHandler extends EventHandler {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private final AuraClient client;

        private final List<Topology.AuraTopology> topologies;

        private final int runs;

        private int jobCounter = 0;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public SubmissionHandler(final AuraClient client, final List<Topology.AuraTopology> topologies, final int runs) {

            this.client = client;

            this.topologies = topologies;

            this.runs = runs;
        }

        // ---------------------------------------------------
        // Private Methods.
        // ---------------------------------------------------

        @EventHandler.Handle(event = IOEvents.ControlIOEvent.class, type = IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED)
        private void handleTopologyFinished(final IOEvents.ControlIOEvent event) {
            if (event != null) {
                String jobName = (String) event.getPayload();
                LOG.error("Topology ({}) finished.", jobName);
            }

            // Each topology is executed #runs times
            if (jobCounter < runs * topologies.size()) {
                final int jobIndex = jobCounter++ / runs;

                Thread t = new Thread() {

                    public void run() {
                        // This break is only necessary to make it easier to distinguish jobs in
                        // the log files.
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

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(String[] args) {

        int machines = 8;

        int cores = 4;

        int runs = 1;

        // Local
        final String zookeeperAddress = "localhost:2181";

        final LocalClusterSimulator lce =
                new LocalClusterSimulator(LocalClusterSimulator.ExecutionMode.EXECUTION_MODE_SINGLE_PROCESS, true, zookeeperAddress, machines);

        // Wally
        // final String zookeeperAddress = "wally101.cit.tu-berlin.de:2181";

        final AuraClient ac = new AuraClient(zookeeperAddress, 10000, 11111);

        final List<Topology.AuraTopology> topologies = buildTopologies(ac, machines, cores);

        final SubmissionHandler handler = new SubmissionHandler(ac, topologies, runs);

        // Add the job resubmission handler.
        ac.ioManager.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED, handler);

        // Start job submission.
        handler.handleTopologyFinished(null);

        try {
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        ac.closeSession();
        // lce.shutdown();
    }
}
