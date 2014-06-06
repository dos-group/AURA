package de.tuberlin.aura.benchmark.runs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.task.spi.AbstractInvokeable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.measurement.AccumulatedLatencyMeasurement;
import de.tuberlin.aura.core.measurement.MeasurementType;
import de.tuberlin.aura.core.measurement.MedianHelper;
import de.tuberlin.aura.core.measurement.record.BenchmarkRecord;
import de.tuberlin.aura.core.measurement.record.Record;
import de.tuberlin.aura.core.task.spi.IDataConsumer;
import de.tuberlin.aura.core.task.spi.IDataProducer;
import de.tuberlin.aura.core.task.spi.ITaskDriver;
import de.tuberlin.aura.core.topology.AuraGraph.AuraTopology;
import de.tuberlin.aura.core.topology.AuraGraph.AuraTopologyBuilder;
import de.tuberlin.aura.core.topology.AuraGraph.Edge;
import de.tuberlin.aura.core.topology.AuraGraph.Node;

public final class BenchmarkClient {

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkClient.class);

    // Disallow Instantiation.
    private BenchmarkClient() {}

    /**
     *
     */
    public static class Task1Exe extends AbstractInvokeable {

        private static final int RECORDS = 10000;

        public Task1Exe(final ITaskDriver taskDriver, IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
            super(taskDriver, producer, consumer, LOG);
        }

        @Override
        public void run() throws Throwable {
            final UUID taskID = driver.getNodeDescriptor().taskID;

            long start = System.nanoTime();

            int i = 0;
            while (i++ < RECORDS && isInvokeableRunning()) {
                final List<Descriptors.AbstractNodeDescriptor> outputs = driver.getBindingDescriptor().outputGateBindings.get(0);
                for (int index = 0; index < outputs.size(); ++index) {
                    final UUID outputTaskID = getTaskID(0, index);

                    final MemoryView buffer = producer.alloc();
                    final IOEvents.TransferBufferEvent outputBuffer = new IOEvents.TransferBufferEvent(taskID, outputTaskID, buffer);

                    final Record<BenchmarkRecord> record = new Record<>(new BenchmarkRecord());

                    driver.getRecordWriter().writeRecord(record, outputBuffer);
                    producer.emit(0, index, outputBuffer);

                    // LOG.debug("Emit at: " + record.getData().time + " " + outputBuffer);
                    buffer.free();
                }
            }

            LOG.info("Emit time: " + Long.toString(Math.abs(System.nanoTime() - start) / 1000000) + " ms ");
        }

        @Override
        public void close() throws Throwable {
            producer.done();
        }
    }

    /**
     *
     */
    public static class Task2Exe extends AbstractInvokeable {

        public Task2Exe(final ITaskDriver taskDriver, IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
            super(taskDriver, producer, consumer, LOG);
        }

        @Override
        public void open() throws Throwable {
            consumer.openGate(0);
        }

        @Override
        public void run() throws Throwable {
            final UUID taskID = driver.getNodeDescriptor().taskID;
            int bufNum = 0;

            while (!consumer.isExhausted() && isInvokeableRunning()) {
                final IOEvents.TransferBufferEvent buffer = consumer.absorb(0);

                // LOG.debug("received: " + buffer + " number " + Integer.toString(++bufNum));

                if (buffer != null) {
                    final Record<BenchmarkRecord> record = driver.getRecordReader().readRecord(buffer);
                    final List<Descriptors.AbstractNodeDescriptor> outputs = driver.getBindingDescriptor().outputGateBindings.get(0);

                    for (int index = 0; index < outputs.size(); ++index) {
                        final UUID outputTaskID = getTaskID(0, index);

                        final MemoryView sendBuffer = producer.alloc();
                        final IOEvents.TransferBufferEvent outputBuffer = new IOEvents.TransferBufferEvent(taskID, outputTaskID, sendBuffer);

                        driver.getRecordWriter().writeRecord(record, outputBuffer);
                        producer.emit(0, index, outputBuffer);

                        // LOG.debug("Emit at: " + record.getData().time);
                    }

                    buffer.buffer.free();
                }
            }
        }

        @Override
        public void close() throws Throwable {
            producer.done();
        }
    }

    /**
     *
     */
    public static class Task3Exe extends AbstractInvokeable {

        public Task3Exe(final ITaskDriver taskDriver, IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
            super(taskDriver, producer, consumer, LOG);
        }

        @Override
        public void open() throws Throwable {
            consumer.openGate(0);
            consumer.openGate(1);
        }

        @Override
        public void run() throws Throwable {
            final UUID taskID = driver.getNodeDescriptor().taskID;

            while (!consumer.isExhausted() && isInvokeableRunning()) {

                final IOEvents.TransferBufferEvent left = consumer.absorb(0);
                final IOEvents.TransferBufferEvent right = consumer.absorb(1);

                if (left != null || right != null) {
                    final Record<BenchmarkRecord> leftRecord = driver.getRecordReader().readRecord(left);
                    final Record<BenchmarkRecord> rightRecord = driver.getRecordReader().readRecord(right);

                    left.buffer.free();
                    right.buffer.free();

                    final List<Descriptors.AbstractNodeDescriptor> outputs = driver.getBindingDescriptor().outputGateBindings.get(0);

                    for (int index = 0; index < outputs.size(); ++index) {

                        final UUID outputTaskID = getTaskID(0, index);

                        // Send right record
                        MemoryView sendBuffer = producer.alloc();
                        IOEvents.TransferBufferEvent outputBuffer = new IOEvents.TransferBufferEvent(taskID, outputTaskID, sendBuffer);

                        driver.getRecordWriter().writeRecord(leftRecord, outputBuffer);
                        producer.emit(0, index, outputBuffer);

                        // Send left record
                        sendBuffer = producer.alloc();
                        outputBuffer = new IOEvents.TransferBufferEvent(taskID, outputTaskID, sendBuffer);

                        driver.getRecordWriter().writeRecord(rightRecord, outputBuffer);
                        producer.emit(0, index, outputBuffer);
                    }
                }
            }
        }

        @Override
        public void close() throws Throwable {
            producer.done();
        }
    }

    /**
     *
     */
    public static class Task4Exe extends AbstractInvokeable {

        public Task4Exe(final ITaskDriver taskDriver, IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
            super(taskDriver, producer, consumer, LOG);
        }

        @Override
        public void open() throws Throwable {
            consumer.openGate(0);
        }

        @Override
        public void run() throws Throwable {
            final UUID taskID = driver.getNodeDescriptor().taskID;

            List<Long> latencies = new LinkedList<>();
            while (!consumer.isExhausted() && isInvokeableRunning()) {
                final IOEvents.TransferBufferEvent buffer = consumer.absorb(0);

                // LOG.debug("received: " + buffer);

                if (buffer != null) {
                    final Record<BenchmarkRecord> record = driver.getRecordReader().readRecord(buffer);

                    long latency = System.currentTimeMillis() - record.getData().time;
                    latencies.add(latency);

                    buffer.buffer.free();
                }
            }

            long latencySum = 0l;
            long minLatency = Long.MAX_VALUE;
            long maxLatency = Long.MIN_VALUE;

            for (long latency : latencies) {
                latencySum += latency;

                if (latency < minLatency) {
                    minLatency = latency;
                }

                if (latency > maxLatency) {
                    maxLatency = latency;
                }
            }

            double avgLatency = (double) latencySum / (double) latencies.size();
            long medianLatency = MedianHelper.findMedian(latencies);

            // LOG.info("RESULTS|" + Double.toString(avgLatency) + "|" + Long.toString(minLatency) +
            // "|" + Long.toString(maxLatency) + "|"
            // + Long.toString(medianLatency));
            driver.getMeasurementManager().add(new AccumulatedLatencyMeasurement(MeasurementType.LATENCY,
                    "Buffer latency",
                    minLatency,
                    maxLatency,
                    avgLatency,
                    medianLatency));
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

        // final String zookeeperAddress = "wally033.cit.tu-berlin.de:2181";
        final String measurementPath = "/home/teots/Desktop/measurements";
        final String zookeeperAddress = "localhost:2181";
        final LocalClusterSimulator lce =
                new LocalClusterSimulator(LocalClusterSimulator.ExecutionMode.EXECUTION_MODE_SINGLE_PROCESS,
                                          true,
                                          zookeeperAddress,
                                          5,
                                          measurementPath);
        final AuraClient ac = new AuraClient(zookeeperAddress, 10000, 11111);

        final AuraTopologyBuilder atb1 = ac.createTopologyBuilder();
        atb1.addNode(new Node(UUID.randomUUID(), "Task1", 1, 1), Task1Exe.class)
            .connectTo("Task2", Edge.TransferType.ALL_TO_ALL)
            .addNode(new Node(UUID.randomUUID(), "Task2", 1, 1), Task2Exe.class)
            .connectTo("Task3", Edge.TransferType.POINT_TO_POINT)
            .addNode(new Node(UUID.randomUUID(), "Task3", 1, 1), Task4Exe.class);

        final AuraTopologyBuilder atb2 = ac.createTopologyBuilder();
        atb2.addNode(new Node(UUID.randomUUID(), "Task1", 2, 1), Task1Exe.class)
            .connectTo("Task3", Edge.TransferType.ALL_TO_ALL)
            .addNode(new Node(UUID.randomUUID(), "Task5", 2, 1), Task1Exe.class)
            .connectTo("Task3", Edge.TransferType.ALL_TO_ALL)
            .addNode(new Node(UUID.randomUUID(), "Task3", 2, 1), Task3Exe.class)
            .connectTo("Task4", Edge.TransferType.POINT_TO_POINT)
            .addNode(new Node(UUID.randomUUID(), "Task4", 2, 1), Task4Exe.class);

        final AuraTopology at1 = atb1.build("Job 1", EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING));
        final AuraTopology at2 = atb2.build("Job 2", EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING));

        for (int i = 0; i < 1; ++i) {
            ac.submitTopology(at1, null);

            try {
                Thread.sleep(30000);
            } catch (InterruptedException e) {
                LOG.error("Sleep interrupted", e);
            }
        }

        try {
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        ac.closeSession();
        lce.shutdown();
    }
}
