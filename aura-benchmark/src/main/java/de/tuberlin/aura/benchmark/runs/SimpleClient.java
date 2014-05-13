package de.tuberlin.aura.benchmark.runs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.task.common.DataConsumer;
import de.tuberlin.aura.core.task.common.DataProducer;
import de.tuberlin.aura.core.task.common.TaskDriverContext;
import de.tuberlin.aura.core.task.common.TaskInvokeable;
import de.tuberlin.aura.core.topology.AuraDirectedGraph;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.Edge;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.Node;

public final class SimpleClient {

    /**
     * Logger.
     */
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SanityClient.class);

    // Disallow Instantiation.
    private SimpleClient() {}

    /**
     *
     */
    public static class Source extends TaskInvokeable {

        private static final int RECORDS = 1000;

        public Source(final TaskDriverContext context, DataProducer producer, final DataConsumer consumer, final Logger LOG) {
            super(context, producer, consumer, LOG);
        }

        @Override
        public void run() throws Throwable {
            final UUID taskID = driverContext.taskDescriptor.taskID;

            int i = 0;
            while (i++ < RECORDS && isInvokeableRunning()) {

                final List<Descriptors.TaskDescriptor> outputs = driverContext.taskBindingDescriptor.outputGateBindings.get(0);
                for (int index = 0; index < outputs.size(); ++index) {
                    final UUID outputTaskID = getTaskID(0, index);

                    final MemoryView buffer = producer.allocBlocking();
                    final IOEvents.TransferBufferEvent event = new IOEvents.TransferBufferEvent(taskID, outputTaskID, buffer);

                    producer.emit(0, index, event);
                }
            }
        }

        @Override
        public void close() throws Throwable {
            LOG.debug("{} {} done", driverContext.taskDescriptor.name, driverContext.taskDescriptor.taskIndex);
            producer.done();
        }
    }

    /**
     *
     */
    public static class ForwardWithOneInput extends TaskInvokeable {

        public ForwardWithOneInput(final TaskDriverContext context, DataProducer producer, final DataConsumer consumer, final Logger LOG) {
            super(context, producer, consumer, LOG);
        }

        @Override
        public void open() throws Throwable {
            consumer.openGate(0);
        }

        @Override
        public void run() throws Throwable {
            final UUID taskID = driverContext.taskDescriptor.taskID;
            final List<Descriptors.TaskDescriptor> outputs = driverContext.taskBindingDescriptor.outputGateBindings.get(0);

            while (!consumer.isExhausted() && isInvokeableRunning()) {
                final IOEvents.TransferBufferEvent event = consumer.absorb(0);

                if (event != null) {

                    event.buffer.free();

                    for (int index = 0; index < outputs.size(); ++index) {
                        final UUID outputTaskID = getTaskID(0, index);

                        final MemoryView sendBuffer = producer.allocBlocking();
                        final IOEvents.TransferBufferEvent outputBuffer = new IOEvents.TransferBufferEvent(taskID, outputTaskID, sendBuffer);

                        producer.emit(0, index, outputBuffer);
                    }


                }
            }
        }

        @Override
        public void close() throws Throwable {
            LOG.debug("{} {} done", driverContext.taskDescriptor.name, driverContext.taskDescriptor.taskIndex);
            producer.done();
        }
    }

    public static class ForwardWithTwoInputs extends TaskInvokeable {

        public ForwardWithTwoInputs(final TaskDriverContext context, DataProducer producer, final DataConsumer consumer, final Logger LOG) {
            super(context, producer, consumer, LOG);
        }

        @Override
        public void open() throws Throwable {
            consumer.openGate(0);
            consumer.openGate(1);
        }

        @Override
        public void run() throws Throwable {
            final UUID taskID = driverContext.taskDescriptor.taskID;
            final List<Descriptors.TaskDescriptor> outputs = driverContext.taskBindingDescriptor.outputGateBindings.get(0);

            while (!consumer.isExhausted() && isInvokeableRunning()) {
                final IOEvents.TransferBufferEvent leftEvent = consumer.absorb(0);
                final IOEvents.TransferBufferEvent rightEvent = consumer.absorb(1);

                if (leftEvent != null) {

                    leftEvent.buffer.free();

                    for (int index = 0; index < outputs.size(); ++index) {
                        final UUID outputTaskID = getTaskID(0, index);

                        final MemoryView sendBuffer = producer.allocBlocking();
                        final IOEvents.TransferBufferEvent outputBuffer = new IOEvents.TransferBufferEvent(taskID, outputTaskID, sendBuffer);

                        producer.emit(0, index, outputBuffer);
                    }


                }

                if (rightEvent != null) {

                    rightEvent.buffer.free();

                    for (int index = 0; index < outputs.size(); ++index) {
                        final UUID outputTaskID = getTaskID(0, index);

                        final MemoryView sendBuffer = producer.allocBlocking();
                        final IOEvents.TransferBufferEvent outputBuffer = new IOEvents.TransferBufferEvent(taskID, outputTaskID, sendBuffer);

                        producer.emit(0, index, outputBuffer);
                    }


                }
            }
        }

        @Override
        public void close() throws Throwable {
            LOG.debug("{} {} done", driverContext.taskDescriptor.name, driverContext.taskDescriptor.taskIndex);
            producer.done();
        }
    }

    /**
     *
     */
    public static class Sink extends TaskInvokeable {

        long count = 0;

        public Sink(final TaskDriverContext context, DataProducer producer, final DataConsumer consumer, final Logger LOG) {
            super(context, producer, consumer, LOG);
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
                    if (count % 100 == 0)
                        LOG.info("Sink receive {}.", count);
                    LOG.info("free in sink");
                    event.buffer.free();
                }
            }

            LOG.info("Sink finished {}.", count);
      }
    }


    // ---------------------------------------------------
    // Main.
    // ---------------------------------------------------

    public static void main(String[] args) {

        int machines = 2;
        int cores = 4;
        int runs = 1;

        // Local
        final String measurementPath = "/home/akunft/local_measurements";
        final String zookeeperAddress = "localhost:2181";
        final LocalClusterSimulator lce =
                new LocalClusterSimulator(LocalClusterSimulator.ExecutionMode.EXECUTION_MODE_SINGLE_PROCESS,
                                          true,
                                          zookeeperAddress,
                                          machines,
                               measurementPath);

 // Wally
        //final String zookeeperAddress = "wally101.cit.tu-berlin.de:2181";

        final AuraClient ac = new AuraClient(zookeeperAddress, 10000, 11111);
        List<AuraTopology> topologies = buildTopologies(ac, machines, cores);
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

    public static List<AuraTopology> buildTopologies(AuraClient client, int machines, int tasksPerMaschine) {
        List<AuraTopology> topologies = new ArrayList<>();

        int executionUnits = machines * tasksPerMaschine;
        AuraDirectedGraph.AuraTopologyBuilder atb;

        // // 2 layered - point2point connection
        // atb = client.createTopologyBuilder();
        // atb.addNode(new Node(UUID.randomUUID(), "Source", executionUnits / 2, 1), Source.class)
        // .connectTo("Sink", Edge.TransferType.POINT_TO_POINT)
        // .addNode(new Node(UUID.randomUUID(), "Sink", executionUnits / 2, 1), Sink.class);
        // topologies.add(atb.build("Job: 2 layered - point2point connection",
        // EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING)));

        // 2 layered - all2all connection
        atb = client.createTopologyBuilder();
        atb.addNode(new Node(UUID.randomUUID(), "Source", executionUnits / 2, 1), Source.class)
           .connectTo("Sink", Edge.TransferType.ALL_TO_ALL)
           .addNode(new Node(UUID.randomUUID(), "Sink", executionUnits / 2, 1), Sink.class);
        topologies.add(atb.build("Job: 2 layered - all2all connection", EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING)));

        // // 3 layered - point2point + point2point connection
        // atb = client.createTopologyBuilder();
        // atb.addNode(new Node(UUID.randomUUID(), "Source", executionUnits / 3, 1), Source.class)
        // .connectTo("Middle", Edge.TransferType.POINT_TO_POINT)
        // .addNode(new Node(UUID.randomUUID(), "Middle", executionUnits / 3, 1),
        // ForwardWithOneInput.class)
        // .connectTo("Sink", Edge.TransferType.POINT_TO_POINT)
        // .addNode(new Node(UUID.randomUUID(), "Sink", executionUnits / 3, 1), Sink.class);
        // topologies.add(atb.build("Job: 3 layered - point2point + point2point connection",
        // EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING)));
        //
        // // 3 layered - all2all + point2point connection
        // atb = client.createTopologyBuilder();
        // atb.addNode(new Node(UUID.randomUUID(), "Source", executionUnits / 3, 1), Source.class)
        // .connectTo("Middle", Edge.TransferType.ALL_TO_ALL)
        // .addNode(new Node(UUID.randomUUID(), "Middle", executionUnits / 3, 1),
        // ForwardWithOneInput.class)
        // .connectTo("Sink", Edge.TransferType.POINT_TO_POINT)
        // .addNode(new Node(UUID.randomUUID(), "Sink", executionUnits / 3, 1), Sink.class);
        // topologies.add(atb.build("Job: 3 layered - all2all + point2point connection",
        // EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING)));
        //
        // // 3 layered - point2point + all2all connection
        // atb = client.createTopologyBuilder();
        // atb.addNode(new Node(UUID.randomUUID(), "Source", executionUnits / 3, 1), Source.class)
        // .connectTo("Middle", Edge.TransferType.POINT_TO_POINT)
        // .addNode(new Node(UUID.randomUUID(), "Middle", executionUnits / 3, 1),
        // ForwardWithOneInput.class)
        // .connectTo("Sink", Edge.TransferType.ALL_TO_ALL)
        // .addNode(new Node(UUID.randomUUID(), "Sink", executionUnits / 3, 1), Sink.class);
        // topologies.add(atb.build("Job: 3 layered - point2point + all2all connection",
        // EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING)));
        //
        // // 3 layered - all2all + all2all connection
        // atb = client.createTopologyBuilder();
        // atb.addNode(new Node(UUID.randomUUID(), "Source", executionUnits / 3, 1), Source.class)
        // .connectTo("Middle", Edge.TransferType.ALL_TO_ALL)
        // .addNode(new Node(UUID.randomUUID(), "Middle", executionUnits / 3, 1),
        // ForwardWithOneInput.class)
        // .connectTo("Sink", Edge.TransferType.ALL_TO_ALL)
        // .addNode(new Node(UUID.randomUUID(), "Sink", executionUnits / 3, 1), Sink.class);
        // topologies.add(atb.build("Job: 3 layered - all2all + all2all connection",
        // EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING)));
        //
        // // 3 layered - point2point (join) point2point connection
        // atb = client.createTopologyBuilder();
        // atb.addNode(new Node(UUID.randomUUID(), "Source Left", executionUnits / 4, 1),
        // Source.class)
        // .connectTo("Middle", Edge.TransferType.POINT_TO_POINT)
        // .addNode(new Node(UUID.randomUUID(), "Source Right", executionUnits / 4, 1),
        // Source.class)
        // .connectTo("Middle", Edge.TransferType.POINT_TO_POINT)
        // .addNode(new Node(UUID.randomUUID(), "Middle", executionUnits / 4, 1),
        // ForwardWithTwoInputs.class)
        // .connectTo("Sink", Edge.TransferType.POINT_TO_POINT)
        // .addNode(new Node(UUID.randomUUID(), "Sink", executionUnits / 4, 1), Sink.class);
        // topologies.add(atb.build("Job: 3 layered - point2point (join) point2point connection",
        // EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING)));
        //
        // // 3 layered - all2all (join) point2point connection
        // atb = client.createTopologyBuilder();
        // atb.addNode(new Node(UUID.randomUUID(), "Source Left", executionUnits / 4, 1),
        // Source.class)
        // .connectTo("Middle", Edge.TransferType.ALL_TO_ALL)
        // .addNode(new Node(UUID.randomUUID(), "Source Right", executionUnits / 4, 1),
        // Source.class)
        // .connectTo("Middle", Edge.TransferType.ALL_TO_ALL)
        // .addNode(new Node(UUID.randomUUID(), "Middle", executionUnits / 4, 1),
        // ForwardWithTwoInputs.class)
        // .connectTo("Sink", Edge.TransferType.POINT_TO_POINT)
        // .addNode(new Node(UUID.randomUUID(), "Sink", executionUnits / 4, 1), Sink.class);
        // topologies.add(atb.build("Job: 3 layered - all2all (join) point2point connection",
        // EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING)));
        //
        // // 3 layered - all2all (join) all2all connection
        // atb = client.createTopologyBuilder();
        // atb.addNode(new Node(UUID.randomUUID(), "Source Left", executionUnits / 4, 1),
        // Source.class)
        // .connectTo("Middle", Edge.TransferType.ALL_TO_ALL)
        // .addNode(new Node(UUID.randomUUID(), "Source Right", executionUnits / 4, 1),
        // Source.class)
        // .connectTo("Middle", Edge.TransferType.ALL_TO_ALL)
        // .addNode(new Node(UUID.randomUUID(), "Middle", executionUnits / 4, 1),
        // ForwardWithTwoInputs.class)
        // .connectTo("Sink", Edge.TransferType.ALL_TO_ALL)
        // .addNode(new Node(UUID.randomUUID(), "Sink", executionUnits / 4, 1), Sink.class);
        // topologies.add(atb.build("Job: 3 layered - all2all (join) all2all connection", EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING)));

        return topologies;
    }

    private static class SubmissionHandler extends EventHandler {

        private final AuraClient client;

        private final List<AuraTopology> topologies;

        private final int runs;

        private int jobCounter = 0;

        public SubmissionHandler(final AuraClient client, final List<AuraTopology> topologies, final int runs) {
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

                        LOG.error("Submit: {} - run {}/{}", topologies.get(jobIndex).name, ((jobCounter - 1) % runs) + 1, runs);
                        client.submitTopology(topologies.get(jobIndex), null);
                    }
                };

                t.start();
            }
        }
    }
}
