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
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.MemoryManager;
import de.tuberlin.aura.core.statistic.MeasurementType;
import de.tuberlin.aura.core.statistic.NumberMeasurement;
import de.tuberlin.aura.core.statistic.record.BenchmarkRecord.SanityBenchmarkRecord;
import de.tuberlin.aura.core.statistic.record.Record;
import de.tuberlin.aura.core.task.common.DataConsumer;
import de.tuberlin.aura.core.task.common.DataProducer;
import de.tuberlin.aura.core.task.common.TaskDriverContext;
import de.tuberlin.aura.core.task.common.TaskInvokeable;
import de.tuberlin.aura.core.topology.AuraDirectedGraph;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.Edge;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.Node;

public final class SanityClient {

    /**
     * Logger.
     */
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SanityClient.class);

    // Disallow Instantiation.
    private SanityClient() {}

    /**
     *
     */
    public static class Source extends TaskInvokeable {

        private static final int RECORDS = 100;

        public Source(final TaskDriverContext context, DataProducer producer, final DataConsumer consumer, final Logger LOG) {
            super(context, producer, consumer, LOG);
        }

        @Override
        public void run() throws Throwable {
            final UUID taskID = driverContext.taskDescriptor.taskID;

            long send = 0l;

            int i = 0;
            while (i++ < RECORDS && isInvokeableRunning()) {

                final List<Descriptors.TaskDescriptor> outputs = driverContext.taskBindingDescriptor.outputGateBindings.get(0);
                for (int index = 0; index < outputs.size(); ++index) {
                    final UUID outputTaskID = getTaskID(0, index);

                    final MemoryManager.MemoryView buffer = producer.alloc();
                    final IOEvents.TransferBufferEvent event = new IOEvents.TransferBufferEvent(taskID, outputTaskID, buffer);

                    final Record<SanityBenchmarkRecord> record = new Record<>(new SanityBenchmarkRecord(outputTaskID));

                    driverContext.recordWriter.writeRecord(record, event);
                    producer.emit(0, index, event);

                    ++send;
                }
            }

            driverContext.measurementManager.add(new NumberMeasurement(MeasurementType.NUMBER, "SOURCE", send));
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

            long bufferCount = 0l;
            long correct = 0l;

            while (!consumer.isExhausted() && isInvokeableRunning()) {
                final IOEvents.TransferBufferEvent event = consumer.absorb(0);

                if (event != null) {
                    final Record<SanityBenchmarkRecord> record = driverContext.recordReader.readRecord(event);
                    ++bufferCount;

                    if (!record.getData().nextTask.equals(driverContext.taskDescriptor.taskID)) {
                        LOG.error("Buffer expected taskID: " + record.getData().nextTask.toString() + " but found "
                                + driverContext.taskDescriptor.taskID + " Task: " + driverContext.taskDescriptor.name);
                    } else {
                        ++correct;
                    }

                    for (int index = 0; index < outputs.size(); ++index) {
                        final UUID outputTaskID = getTaskID(0, index);

                        final MemoryManager.MemoryView sendBuffer = producer.alloc();
                        final IOEvents.TransferBufferEvent outputBuffer = new IOEvents.TransferBufferEvent(taskID, outputTaskID, sendBuffer);

                        record.getData().nextTask = outputTaskID;

                        driverContext.recordWriter.writeRecord(record, outputBuffer);
                        producer.emit(0, index, outputBuffer);
                    }

                    event.buffer.free();
                }
            }

            driverContext.measurementManager.add(new NumberMeasurement(MeasurementType.NUMBER, "BUFFERS", bufferCount));
            driverContext.measurementManager.add(new NumberMeasurement(MeasurementType.NUMBER, "CORRECT_BUFFERS", correct));
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

            long bufferCount = 0l;
            long correct = 0l;

            while (!consumer.isExhausted() && isInvokeableRunning()) {
                final IOEvents.TransferBufferEvent leftEvent = consumer.absorb(0);
                final IOEvents.TransferBufferEvent rightEvent = consumer.absorb(1);

                if (leftEvent != null) {
                    final Record<SanityBenchmarkRecord> leftRecord = driverContext.recordReader.readRecord(leftEvent);
                    ++bufferCount;

                    if (!leftRecord.getData().nextTask.equals(driverContext.taskDescriptor.taskID)) {
                        LOG.error("Buffer expected taskID: " + leftRecord.getData().nextTask.toString() + " but found "
                                + driverContext.taskDescriptor.taskID + " Task: " + driverContext.taskDescriptor.name);
                    } else {
                        ++correct;
                    }

                    for (int index = 0; index < outputs.size(); ++index) {
                        final UUID outputTaskID = getTaskID(0, index);

                        final MemoryManager.MemoryView sendBuffer = producer.alloc();
                        final IOEvents.TransferBufferEvent outputBuffer = new IOEvents.TransferBufferEvent(taskID, outputTaskID, sendBuffer);

                        leftRecord.getData().nextTask = outputTaskID;

                        driverContext.recordWriter.writeRecord(leftRecord, outputBuffer);
                        producer.emit(0, index, outputBuffer);
                    }

                    leftEvent.buffer.free();
                }

                if (rightEvent != null) {
                    final Record<SanityBenchmarkRecord> rightRecord = driverContext.recordReader.readRecord(leftEvent);
                    ++bufferCount;

                    if (!rightRecord.getData().nextTask.equals(driverContext.taskDescriptor.taskID)) {
                        LOG.error("Buffer expected taskID: " + rightRecord.getData().nextTask.toString() + " but found "
                                + driverContext.taskDescriptor.taskID + " Task: " + driverContext.taskDescriptor.name);
                    } else {
                        ++correct;
                    }

                    for (int index = 0; index < outputs.size(); ++index) {
                        final UUID outputTaskID = getTaskID(0, index);

                        final MemoryManager.MemoryView sendBuffer = producer.alloc();
                        final IOEvents.TransferBufferEvent outputBuffer = new IOEvents.TransferBufferEvent(taskID, outputTaskID, sendBuffer);

                        rightRecord.getData().nextTask = outputTaskID;

                        driverContext.recordWriter.writeRecord(rightRecord, outputBuffer);
                        producer.emit(0, index, outputBuffer);
                    }

                    rightEvent.buffer.free();
                }
            }

            driverContext.measurementManager.add(new NumberMeasurement(MeasurementType.NUMBER, "BUFFERS", bufferCount));
            driverContext.measurementManager.add(new NumberMeasurement(MeasurementType.NUMBER, "CORRECT_BUFFERS", correct));
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

        public Sink(final TaskDriverContext context, DataProducer producer, final DataConsumer consumer, final Logger LOG) {
            super(context, producer, consumer, LOG);
        }

        @Override
        public void open() throws Throwable {
            consumer.openGate(0);
        }

        @Override
        public void run() throws Throwable {
            final UUID taskID = driverContext.taskDescriptor.taskID;
            long receivedRecords = 0l;

            while (!consumer.isExhausted() && isInvokeableRunning()) {
                final IOEvents.TransferBufferEvent event = consumer.absorb(0);

                if (event != null) {
                    final Record<SanityBenchmarkRecord> record = driverContext.recordReader.readRecord(event);
                    if (!record.getData().nextTask.equals(driverContext.taskDescriptor.taskID)) {
                        LOG.error("Buffer expected taskID: " + record.getData().nextTask.toString() + " but found "
                                + driverContext.taskDescriptor.taskID + " Task: " + driverContext.taskDescriptor.name);
                    }

                    ++receivedRecords;
                    event.buffer.free();
                }
            }

            driverContext.measurementManager.add(new NumberMeasurement(MeasurementType.NUMBER, "SINK", receivedRecords));
        }
    }


    // ---------------------------------------------------
    // Main.
    // ---------------------------------------------------

    public static void main(String[] args) {

        // final SimpleLayout layout = new SimpleLayout();
        // final ConsoleAppender consoleAppender = new ConsoleAppender(layout);
        // LOG.addAppender(consoleAppender);
        // LOG.setLevel(Level.INFO);

        int machines = 99;

        // Local
        // final String measurementPath = "/home/teots/Desktop/logs";
        // final String zookeeperAddress = "localhost:2181";
        // final LocalClusterSimulator lce =
        // new
        // LocalClusterSimulator(LocalClusterSimulator.ExecutionMode.EXECUTION_MODE_SINGLE_PROCESS,
        // true,
        // zookeeperAddress,
        // machines,
        // measurementPath);

        // Wally
        final String zookeeperAddress = "wally101.cit.tu-berlin.de:2181";

        final AuraClient ac = new AuraClient(zookeeperAddress, 10000, 11111);
        List<AuraTopology> topologies = buildTopologies(ac, machines, 8);
        SubmissionHandler handler = new SubmissionHandler(ac, topologies, 1);

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

        // 2 layered - point2point connection
        atb = client.createTopologyBuilder();
        atb.addNode(new Node(UUID.randomUUID(), "Source", executionUnits / 2, 1), Source.class)
           .connectTo("Sink", Edge.TransferType.POINT_TO_POINT)
           .addNode(new Node(UUID.randomUUID(), "Sink", executionUnits / 2, 1), Sink.class);
        topologies.add(atb.build("Job: 2 layered - point2point connection", EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING)));

        // 2 layered - all2all connection
        atb = client.createTopologyBuilder();
        atb.addNode(new Node(UUID.randomUUID(), "Source", executionUnits / 2, 1), Source.class)
           .connectTo("Sink", Edge.TransferType.ALL_TO_ALL)
           .addNode(new Node(UUID.randomUUID(), "Sink", executionUnits / 2, 1), Sink.class);
        topologies.add(atb.build("Job: 2 layered - all2all connection", EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING)));

        // 3 layered - point2point + point2point connection
        atb = client.createTopologyBuilder();
        atb.addNode(new Node(UUID.randomUUID(), "Source", executionUnits / 3, 1), Source.class)
           .connectTo("Middle", Edge.TransferType.POINT_TO_POINT)
           .addNode(new Node(UUID.randomUUID(), "Middle", executionUnits / 3, 1), ForwardWithOneInput.class)
           .connectTo("Sink", Edge.TransferType.POINT_TO_POINT)
           .addNode(new Node(UUID.randomUUID(), "Sink", executionUnits / 3, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - point2point + point2point connection", EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING)));

        // 3 layered - all2all + point2point connection
        atb = client.createTopologyBuilder();
        atb.addNode(new Node(UUID.randomUUID(), "Source", executionUnits / 3, 1), Source.class)
           .connectTo("Middle", Edge.TransferType.ALL_TO_ALL)
           .addNode(new Node(UUID.randomUUID(), "Middle", executionUnits / 3, 1), ForwardWithOneInput.class)
           .connectTo("Sink", Edge.TransferType.POINT_TO_POINT)
           .addNode(new Node(UUID.randomUUID(), "Sink", executionUnits / 3, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - all2all + point2point connection", EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING)));

        // 3 layered - point2point + all2all connection
        atb = client.createTopologyBuilder();
        atb.addNode(new Node(UUID.randomUUID(), "Source", executionUnits / 3, 1), Source.class)
           .connectTo("Middle", Edge.TransferType.POINT_TO_POINT)
           .addNode(new Node(UUID.randomUUID(), "Middle", executionUnits / 3, 1), ForwardWithOneInput.class)
           .connectTo("Sink", Edge.TransferType.ALL_TO_ALL)
           .addNode(new Node(UUID.randomUUID(), "Sink", executionUnits / 3, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - point2point + all2all connection", EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING)));

        // 3 layered - all2all + all2all connection
        atb = client.createTopologyBuilder();
        atb.addNode(new Node(UUID.randomUUID(), "Source", executionUnits / 3, 1), Source.class)
           .connectTo("Middle", Edge.TransferType.ALL_TO_ALL)
           .addNode(new Node(UUID.randomUUID(), "Middle", executionUnits / 3, 1), ForwardWithOneInput.class)
           .connectTo("Sink", Edge.TransferType.ALL_TO_ALL)
           .addNode(new Node(UUID.randomUUID(), "Sink", executionUnits / 3, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - all2all + all2all connection", EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING)));

        // 3 layered - point2point (join) point2point connection
        atb = client.createTopologyBuilder();
        atb.addNode(new Node(UUID.randomUUID(), "Source Left", executionUnits / 4, 1), Source.class)
           .connectTo("Middle", Edge.TransferType.POINT_TO_POINT)
           .addNode(new Node(UUID.randomUUID(), "Source Right", executionUnits / 4, 1), Source.class)
           .connectTo("Middle", Edge.TransferType.POINT_TO_POINT)
           .addNode(new Node(UUID.randomUUID(), "Middle", executionUnits / 4, 1), ForwardWithTwoInputs.class)
           .connectTo("Sink", Edge.TransferType.POINT_TO_POINT)
           .addNode(new Node(UUID.randomUUID(), "Sink", executionUnits / 4, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - point2point (join) point2point connection", EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING)));

        // 3 layered - all2all (join) point2point connection
        atb = client.createTopologyBuilder();
        atb.addNode(new Node(UUID.randomUUID(), "Source Left", executionUnits / 4, 1), Source.class)
           .connectTo("Middle", Edge.TransferType.ALL_TO_ALL)
           .addNode(new Node(UUID.randomUUID(), "Source Right", executionUnits / 4, 1), Source.class)
           .connectTo("Middle", Edge.TransferType.ALL_TO_ALL)
           .addNode(new Node(UUID.randomUUID(), "Middle", executionUnits / 4, 1), ForwardWithTwoInputs.class)
           .connectTo("Sink", Edge.TransferType.POINT_TO_POINT)
           .addNode(new Node(UUID.randomUUID(), "Sink", executionUnits / 4, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - all2all (join) point2point connection", EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING)));

        // 3 layered - all2all (join) all2all connection
        atb = client.createTopologyBuilder();
        atb.addNode(new Node(UUID.randomUUID(), "Source Left", executionUnits / 4, 1), Source.class)
           .connectTo("Middle", Edge.TransferType.ALL_TO_ALL)
           .addNode(new Node(UUID.randomUUID(), "Source Right", executionUnits / 4, 1), Source.class)
           .connectTo("Middle", Edge.TransferType.ALL_TO_ALL)
           .addNode(new Node(UUID.randomUUID(), "Middle", executionUnits / 4, 1), ForwardWithTwoInputs.class)
           .connectTo("Sink", Edge.TransferType.ALL_TO_ALL)
           .addNode(new Node(UUID.randomUUID(), "Sink", executionUnits / 4, 1), Sink.class);
        topologies.add(atb.build("Job: 3 layered - all2all (join) all2all connection", EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING)));

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
                LOG.info("Topology ({}) finished.", jobName);
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

                        LOG.info("Submit: {}", topologies.get(jobIndex).name);
                        client.submitTopology(topologies.get(jobIndex), null);
                    }
                };

                t.start();
            }
        }
    }
}
