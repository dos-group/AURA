package de.tuberlin.aura.benchmark.runs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
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
import de.tuberlin.aura.core.memory.BufferAllocator;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.task.common.*;
import de.tuberlin.aura.core.topology.AuraDirectedGraph;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.Edge;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.Node;

public final class CountClient {

    /**
     * Logger.
     */
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SanityClient.class);

    // Disallow Instantiation.
    private CountClient() {}

    /**
     *
     */
    public static class Source extends TaskInvokeable {

        private final TaskRecordWriter writer;

        private final TaskRecordReader reader;


        private static final long RECORDS = 10;

        public Source(final TaskDriverContext context, DataProducer producer, final DataConsumer consumer, final Logger LOG) {
            super(context, producer, consumer, LOG);
            writer = new TaskRecordWriter(BufferAllocator._64K);
            reader = new TaskRecordReader(BufferAllocator._64K);
        }

        @Override
        public void run() throws Throwable {
            final UUID taskID = driverContext.taskDescriptor.taskID;

            long i = 0;
            while (i++ < RECORDS && isInvokeableRunning()) {

                final List<Descriptors.TaskDescriptor> outputs = driverContext.taskBindingDescriptor.outputGateBindings.get(0);
                for (int index = 0; index < outputs.size(); ++index) {
                    final UUID outputTaskID = getTaskID(0, index);

                    final MemoryView buffer = producer.allocBlocking();
                    writer.selectBuffer(buffer);
                    writer.writeRecord(i);

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

        long count = 0;

        long out = 0;

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
                    count++;
                    ByteBuffer byteBuf = ByteBuffer.wrap(event.buffer.memory, event.buffer.baseOffset, event.buffer.size);
                    final long value = byteBuf.getLong();
                    if (value != count) {
                        LOG.error("expected: " + count + ", but was: " + value);
                    }

                    event.buffer.free();

                    for (int index = 0; index < outputs.size(); ++index) {
                        final UUID outputTaskID = getTaskID(0, index);

                        final MemoryView sendBuffer = producer.allocBlocking();
                        ByteBuffer outBuf = ByteBuffer.wrap(sendBuffer.memory, sendBuffer.baseOffset, sendBuffer.size);
                        outBuf.putLong(++out);
                        outBuf.flip();
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

        long countLeft = 0;

        long countRight = 0;

        long out = 0;

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
                    countLeft++;
                    ByteBuffer byteBuf = ByteBuffer.wrap(leftEvent.buffer.memory, leftEvent.buffer.baseOffset, leftEvent.buffer.size);
                    final long value = byteBuf.getLong();
                    if (value != countLeft) {
                        LOG.error("left expected: " + countLeft + ", but was: " + value);
                    }

                    leftEvent.buffer.free();

                    for (int index = 0; index < outputs.size(); ++index) {
                        final UUID outputTaskID = getTaskID(0, index);

                        final MemoryView sendBuffer = producer.allocBlocking();
                        ByteBuffer outBuf = ByteBuffer.wrap(sendBuffer.memory, sendBuffer.baseOffset, sendBuffer.size);
                        outBuf.putLong(++out);
                        outBuf.flip();
                        final IOEvents.TransferBufferEvent outputBuffer = new IOEvents.TransferBufferEvent(taskID, outputTaskID, sendBuffer);

                        producer.emit(0, index, outputBuffer);
                    }


                }

                if (rightEvent != null) {
                    countRight++;
                    ByteBuffer byteBuf = ByteBuffer.wrap(rightEvent.buffer.memory);
                    final long value = byteBuf.getLong();
                    if (value != countRight) {
                        LOG.error("right expected: " + countRight + ", but was: " + value);
                    }

                    rightEvent.buffer.free();

                    for (int index = 0; index < outputs.size(); ++index) {
                        final UUID outputTaskID = getTaskID(0, index);

                        final MemoryView sendBuffer = producer.allocBlocking();
                        ByteBuffer outBuf = ByteBuffer.wrap(sendBuffer.memory, sendBuffer.baseOffset, sendBuffer.size);
                        outBuf.putLong(++out);
                        outBuf.flip();
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

        private final TaskRecordReader recordReader;

        public Sink(final TaskDriverContext context, DataProducer producer, final DataConsumer consumer, final Logger LOG) {
            super(context, producer, consumer, LOG);

            recordReader = new TaskRecordReader(BufferAllocator._64K);
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

                    recordReader.selectBuffer(event.buffer);
                    long value = recordReader.readRecord(Long.class);
                    if (value != count) {
                        LOG.error("expected: " + count + ", but was: " + value);
                    }


                    if (count % 10000 == 0)
                        LOG.info("Sink receive {}.", count);
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
    public static class SinkWithTwoInputs extends TaskInvokeable {

        long countLeft = 0;

        long countRight = 0;

        private final TaskRecordReader recordReaderLeft;

        private final TaskRecordReader recordReaderRight;

        public SinkWithTwoInputs(final TaskDriverContext context, DataProducer producer, final DataConsumer consumer, final Logger LOG) {
            super(context, producer, consumer, LOG);

            recordReaderLeft = new TaskRecordReader(BufferAllocator._64K);

            recordReaderRight = new TaskRecordReader(BufferAllocator._64K);
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
                    countLeft++;
                    recordReaderLeft.selectBuffer(left.buffer);
                    long value = recordReaderLeft.readRecord(Long.class);

                    if (value != countLeft) {
                        LOG.error("left expected: " + countLeft + ", but was: " + value);
                    }

                    if (countLeft % 10000 == 0)
                        LOG.info("Sink left receive {}.", countLeft);
                    // LOG.info("free in sink");
                    left.buffer.free();
                }
                if (right != null) {
                    countRight++;
                    recordReaderRight.selectBuffer(right.buffer);
                    long value = recordReaderRight.readRecord(Long.class);

                    if (value != countRight) {
                        LOG.error("right expected: " + countRight + ", but was: " + value);
                    }

                    if (countRight % 10000 == 0)
                        LOG.info("Sink right receive {}.", countRight);
                    // LOG.info("free in sink");
                    right.buffer.free();
                }
            }

            LOG.info("Sink finished {}.", (countLeft + countRight));
        }
    }


    // ---------------------------------------------------
    // Main.
    // ---------------------------------------------------

    public static void main(String[] args) {

        int machines = 4;
        int cores = 1;
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
        // final String zookeeperAddress = "wally101.cit.tu-berlin.de:2181";

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
        //
        // // 2 layered - all2all connection
        // atb = client.createTopologyBuilder();
        // atb.addNode(new Node(UUID.randomUUID(), "Source", executionUnits / 2, 1), Source.class)
        // .connectTo("Sink", Edge.TransferType.ALL_TO_ALL)
        // .addNode(new Node(UUID.randomUUID(), "Sink", executionUnits / 2, 1), Sink.class);
        // topologies.add(atb.build("Job: 2 layered - all2all connection",
        // EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING)));

        // 3 layered - all2all (join) all2all connection
        atb = client.createTopologyBuilder();
        atb.addNode(new Node(UUID.randomUUID(), "Source Left", executionUnits / 3, 1), Source.class)
           .connectTo("Sink", Edge.TransferType.ALL_TO_ALL)
           .addNode(new Node(UUID.randomUUID(), "Source Right", executionUnits / 3, 1), Source.class)
           .connectTo("Sink", Edge.TransferType.ALL_TO_ALL)
           .addNode(new Node(UUID.randomUUID(), "Sink", executionUnits / 2, 1), Sink.class);
        topologies.add(atb.build("Job: 2 layered - all2all (join) connection", EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING)));
        //
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

        // // 3 layered - all2all (join) all2all connection
        // atb = client.createTopologyBuilder();
        // atb.addNode(new Node(UUID.randomUUID(), "Source Left", executionUnits / 3, 1),
        // Source.class)
        // .connectTo("Middle", Edge.TransferType.ALL_TO_ALL)
        // .addNode(new Node(UUID.randomUUID(), "Source Right", executionUnits / 3, 1),
        // Source.class)
        // .connectTo("Middle", Edge.TransferType.ALL_TO_ALL)
        // .addNode(new Node(UUID.randomUUID(), "Middle", executionUnits / 3, 1),
        // ForwardWithTwoInputs.class)
        // .connectTo("Sink", Edge.TransferType.ALL_TO_ALL)
        // .addNode(new Node(UUID.randomUUID(), "Sink", executionUnits / 4, 1), Sink.class);
        // topologies.add(atb.build("Job: 3 layered - all2all (join) all2all connection",
        // EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING)));

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
