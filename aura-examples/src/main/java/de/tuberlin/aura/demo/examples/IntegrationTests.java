package de.tuberlin.aura.demo.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import de.tuberlin.aura.core.task.spi.AbstractInvokeable;
import de.tuberlin.aura.core.task.spi.IDataConsumer;
import de.tuberlin.aura.core.task.spi.IDataProducer;
import de.tuberlin.aura.core.task.spi.ITaskDriver;
import de.tuberlin.aura.core.topology.AuraGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.MemoryView;

public final class IntegrationTests {

    /**
     * Logger.
     */
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SimpleClient.class);

    // Disallow Instantiation.
    private IntegrationTests() {}

    /**
     *
     */
    public static class SmallSource extends AbstractInvokeable {

        private static final int RECORDS = 5;

        public SmallSource(final ITaskDriver driver, IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
            super(driver, producer, consumer, LOG);
        }

        @Override
        public void run() throws Throwable {
            final UUID taskID = driver.getNodeDescriptor().taskID;

            int i = 0;
            while (i++ < RECORDS && isInvokeableRunning()) {

                final List<Descriptors.AbstractNodeDescriptor> outputs = driver.getBindingDescriptor().outputGateBindings.get(0);
                final MemoryView buffer = producer.getAllocator().allocBlocking();
                for (int index = 0; index < outputs.size(); ++index) {
                    final UUID outputTaskID = getTaskID(0, index);

                    buffer.retain();
                    final IOEvents.TransferBufferEvent event = new IOEvents.TransferBufferEvent(taskID, outputTaskID, buffer);

                    //System.out.println("------------------> " + taskID);

                    producer.emit(0, index, event);
                }
            }

            LOG.error("Source finished");
        }

        @Override
        public void close() throws Throwable {
            LOG.debug("{} {} done", driver.getNodeDescriptor().name, driver.getNodeDescriptor().taskIndex);
            producer.done();
        }
    }

    public static class LargeSource extends AbstractInvokeable {

        private static final int RECORDS = 10;

        public LargeSource(final ITaskDriver driver, IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
            super(driver, producer, consumer, LOG);
        }

        @Override
        public void run() throws Throwable {
            final UUID taskID = driver.getNodeDescriptor().taskID;

            int i = 0;
            while (i++ < RECORDS && isInvokeableRunning()) {

                final List<Descriptors.AbstractNodeDescriptor> outputs = driver.getBindingDescriptor().outputGateBindings.get(0);
                final MemoryView buffer = producer.getAllocator().allocBlocking();
                for (int index = 0; index < outputs.size(); ++index) {
                    final UUID outputTaskID = getTaskID(0, index);

                    buffer.retain();
                    final IOEvents.TransferBufferEvent event = new IOEvents.TransferBufferEvent(taskID, outputTaskID, buffer);

                    //System.out.println("------------------> " + taskID);

                    producer.emit(0, index, event);
                }
            }

            LOG.error("Source finished");
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
            final UUID taskID = driver.getNodeDescriptor().taskID;
            final List<Descriptors.AbstractNodeDescriptor> outputs = driver.getBindingDescriptor().outputGateBindings.get(0);

            while (!consumer.isExhausted() && isInvokeableRunning()) {
                final IOEvents.TransferBufferEvent event = consumer.absorb(0);

                if (event != null) {

                    event.buffer.free();

                    final MemoryView sendBuffer = producer.getAllocator().allocBlocking();
                    for (int index = 0; index < outputs.size(); ++index) {
                        final UUID outputTaskID = getTaskID(0, index);

                        sendBuffer.retain();
                        final IOEvents.TransferBufferEvent outputBuffer = new IOEvents.TransferBufferEvent(taskID, outputTaskID, sendBuffer);

                        producer.emit(0, index, outputBuffer);
                    }


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
            final UUID taskID = driver.getNodeDescriptor().taskID;
            final List<Descriptors.AbstractNodeDescriptor> outputs = driver.getBindingDescriptor().outputGateBindings.get(0);

            while (!consumer.isExhausted() && isInvokeableRunning()) {
                final IOEvents.TransferBufferEvent leftEvent = consumer.absorb(0);
                final IOEvents.TransferBufferEvent rightEvent = consumer.absorb(1);

                if (leftEvent != null) {

                    leftEvent.buffer.free();

                    final MemoryView sendBuffer = producer.getAllocator().allocBlocking();
                    for (int index = 0; index < outputs.size(); ++index) {
                        final UUID outputTaskID = getTaskID(0, index);

                        sendBuffer.retain();
                        final IOEvents.TransferBufferEvent outputBuffer = new IOEvents.TransferBufferEvent(taskID, outputTaskID, sendBuffer);

                        producer.emit(0, index, outputBuffer);
                    }
                }

                if (rightEvent != null) {

                    rightEvent.buffer.free();

                    final MemoryView sendBuffer = producer.getAllocator().allocBlocking();
                    for (int index = 0; index < outputs.size(); ++index) {
                        final UUID outputTaskID = getTaskID(0, index);

                        sendBuffer.retain();
                        final IOEvents.TransferBufferEvent outputBuffer = new IOEvents.TransferBufferEvent(taskID, outputTaskID, sendBuffer);

                        producer.emit(0, index, outputBuffer);
                    }
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
                    // if (count % 10000 == 0)
                    // LOG.info("Sink receive {}.", count);
                    // LOG.info("free in sink");
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
                    // LOG.info("Sink left receive {}.", count);
                    // LOG.info("free in sink");
                    left.buffer.free();
                }
                if (right != null) {
                    count++;
                    // if (count % 10000 == 0)
                    // LOG.info("Sink right receive {}.", count);
                    // LOG.info("free in sink");
                    right.buffer.free();
                }
            }

            LOG.info("Sink finished {}.", count);
        }
    }


    // ---------------------------------------------------
    // Main.
    // ---------------------------------------------------

    public static void main(String[] args) {

        int machines = 8;
        int cores = 4;
        int runs = 35;

        // Local
        final String zookeeperAddress = "localhost:2181";
        final LocalClusterSimulator lce =
                new LocalClusterSimulator(LocalClusterSimulator.ExecutionMode.EXECUTION_MODE_SINGLE_PROCESS,
                        true,
                        zookeeperAddress,
                        machines);

        // Wally
        // final String zookeeperAddress = "wally101.cit.tu-berlin.de:2181";

        final AuraClient ac = new AuraClient(zookeeperAddress, 10000, 11111);
        List<AuraGraph.AuraTopology> topologies = buildTopologies(ac, machines, cores);
        SubmissionHandler handler = new SubmissionHandler(ac, topologies, runs);

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

    public static List<AuraGraph.AuraTopology> buildTopologies(AuraClient client, int machines, int tasksPerMaschine) {
        List<AuraGraph.AuraTopology> topologies = new ArrayList<>();

        int executionUnits = machines * tasksPerMaschine;
        AuraGraph.AuraTopologyBuilder atb;

        // // 2 layered - point2point connection
        /*atb = client.createTopologyBuilder();
        atb.addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Source", 1, 1), LargeSource.class)
                .connectTo("Sink", AuraGraph.Edge.TransferType.POINT_TO_POINT)
                .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Sink", 1, 1), Sink.class);
        topologies.add(atb.build("Job: 2 layered - point2point connection", EnumSet.of(AuraGraph.AuraTopology.MonitoringType.NO_MONITORING)));

        // 2 layered - all2all connection
        atb = client.createTopologyBuilder();
        atb.addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Source", executionUnits / 2, 1),
        LargeSource.class)
        .connectTo("Sink", AuraGraph.Edge.TransferType.ALL_TO_ALL)
        .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 2, 1), Sink.class);
        topologies.add(atb.build("Job: 2 layered - all2all connection",
        EnumSet.of(AuraGraph.AuraTopology.MonitoringType.NO_MONITORING)));

        // 3 layered - point2point + point2point connection
        atb = client.createTopologyBuilder();
        atb.addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Source", executionUnits / 3, 1),
        LargeSource.class)
        .connectTo("Middle", AuraGraph.Edge.TransferType.POINT_TO_POINT)
        .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Middle", executionUnits / 3, 1),
        ForwardWithOneInput.class)
        .connectTo("Sink", AuraGraph.Edge.TransferType.POINT_TO_POINT)
        .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 3, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - point2point + point2point connection",
        EnumSet.of(AuraGraph.AuraTopology.MonitoringType.NO_MONITORING)));

        // 3 layered - all2all + point2point connection
        atb = client.createTopologyBuilder();
        atb.addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Source", executionUnits / 3, 1),
        LargeSource.class)
        .connectTo("Middle", AuraGraph.Edge.TransferType.ALL_TO_ALL)
        .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Middle", executionUnits / 3, 1),
        ForwardWithOneInput.class)
        .connectTo("Sink", AuraGraph.Edge.TransferType.POINT_TO_POINT)
        .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 3, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - all2all + point2point connection",
        EnumSet.of(AuraGraph.AuraTopology.MonitoringType.NO_MONITORING)));

        // 3 layered - point2point + all2all connection
        atb = client.createTopologyBuilder();
        atb.addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Source", executionUnits / 3, 1),
        LargeSource.class)
        .connectTo("Middle", AuraGraph.Edge.TransferType.POINT_TO_POINT)
        .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Middle", executionUnits / 3, 1),
        ForwardWithOneInput.class)
        .connectTo("Sink", AuraGraph.Edge.TransferType.ALL_TO_ALL)
        .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 3, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - point2point + all2all connection",
        EnumSet.of(AuraGraph.AuraTopology.MonitoringType.NO_MONITORING)));

        // 3 layered - all2all + all2all connection
        atb = client.createTopologyBuilder();
        atb.addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Source", executionUnits / 3, 1),
        LargeSource.class)
        .connectTo("Middle", AuraGraph.Edge.TransferType.ALL_TO_ALL)
        .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Middle", executionUnits / 3, 1),
        ForwardWithOneInput.class)
        .connectTo("Sink", AuraGraph.Edge.TransferType.ALL_TO_ALL)
        .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 3, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - all2all + all2all connection",
        EnumSet.of(AuraGraph.AuraTopology.MonitoringType.NO_MONITORING)));

        // 3 layered - point2point (join) point2point connection
        atb = client.createTopologyBuilder();
        atb.addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Source Left", executionUnits / 4, 1),
        LargeSource.class)
        .connectTo("Middle", AuraGraph.Edge.TransferType.POINT_TO_POINT)
        .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Source Right", executionUnits / 4, 1),
        LargeSource.class)
        .connectTo("Middle", AuraGraph.Edge.TransferType.POINT_TO_POINT)
        .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Middle", executionUnits / 4, 1),
        ForwardWithTwoInputs.class)
        .connectTo("Sink", AuraGraph.Edge.TransferType.POINT_TO_POINT)
        .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 4, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - point2point (join) point2point connection",
        EnumSet.of(AuraGraph.AuraTopology.MonitoringType.NO_MONITORING)));

        // 3 layered - all2all (join) point2point connection
        atb = client.createTopologyBuilder();
        atb.addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Source Left", executionUnits / 4, 1),
        LargeSource.class)
        .connectTo("Middle", AuraGraph.Edge.TransferType.ALL_TO_ALL)
        .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Source Right", executionUnits / 4, 1),
        LargeSource.class)
        .connectTo("Middle", AuraGraph.Edge.TransferType.ALL_TO_ALL)
        .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Middle", executionUnits / 4, 1),
        ForwardWithTwoInputs.class)
        .connectTo("Sink", AuraGraph.Edge.TransferType.POINT_TO_POINT)
        .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 4, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - all2all (join) point2point connection",
        EnumSet.of(AuraGraph.AuraTopology.MonitoringType.NO_MONITORING)));

        // 3 layered - all2all (join) all2all connection
        atb = client.createTopologyBuilder();
        atb.addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Source Left", executionUnits / 4, 1),
        LargeSource.class)
        .connectTo("Middle", AuraGraph.Edge.TransferType.ALL_TO_ALL)
        .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Source Right", executionUnits / 4, 1),
        LargeSource.class)
        .connectTo("Middle", AuraGraph.Edge.TransferType.ALL_TO_ALL)
        .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Middle", executionUnits / 4, 1),
        ForwardWithTwoInputs.class)
        .connectTo("Sink", AuraGraph.Edge.TransferType.ALL_TO_ALL)
        .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 4, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - all2all (join) all2all connection",
        EnumSet.of(AuraGraph.AuraTopology.MonitoringType.NO_MONITORING)));

        // 3 layered - all2all (join) all2all connection (small/large)
        atb = client.createTopologyBuilder();
        atb.addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Source Left", executionUnits / 4, 1),
        LargeSource.class)
        .connectTo("Middle", AuraGraph.Edge.TransferType.ALL_TO_ALL)
        .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Source Right", executionUnits / 4, 1),
        SmallSource.class)
        .connectTo("Middle", AuraGraph.Edge.TransferType.ALL_TO_ALL)
        .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Middle", executionUnits / 4, 1),
        ForwardWithTwoInputs.class)
        .connectTo("Sink", AuraGraph.Edge.TransferType.ALL_TO_ALL)
        .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 4, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - all2all (join) all2all connection (small/large)",
        EnumSet.of(AuraGraph.AuraTopology.MonitoringType.NO_MONITORING)));*/

        // 6 layered
        atb = client.createTopologyBuilder();
        atb.addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Source Left", executionUnits / 6, 1), SmallSource.class)
           .connectTo("Middle", AuraGraph.Edge.TransferType.ALL_TO_ALL)
           .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Source Right", executionUnits / 6, 1), LargeSource.class)
           .connectTo("Middle", AuraGraph.Edge.TransferType.ALL_TO_ALL)
           .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Middle", executionUnits / 6, 1), ForwardWithTwoInputs.class)
           .connectTo("Middle2", AuraGraph.Edge.TransferType.ALL_TO_ALL)
           .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Source Middle", executionUnits / 6, 1), SmallSource.class)
           .connectTo("Middle2", AuraGraph.Edge.TransferType.ALL_TO_ALL)
           .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Middle2", executionUnits / 6, 1), ForwardWithTwoInputs.class)
           .connectTo("Sink", AuraGraph.Edge.TransferType.ALL_TO_ALL)
           .addNode(new AuraGraph.ComputationNode(UUID.randomUUID(), "Sink", executionUnits / 6, 1), Sink.class);
        topologies.add(atb.build("Job: 6 layered", EnumSet.of(AuraGraph.AuraTopology.MonitoringType.NO_MONITORING)));

        return topologies;
    }

    private static class SubmissionHandler extends EventHandler {

        private final AuraClient client;

        private final List<AuraGraph.AuraTopology> topologies;

        private final int runs;

        private int jobCounter = 0;

        public SubmissionHandler(final AuraClient client, final List<AuraGraph.AuraTopology> topologies, final int runs) {
            this.client = client;
            this.topologies = topologies;
            this.runs = runs;
        }

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
}
