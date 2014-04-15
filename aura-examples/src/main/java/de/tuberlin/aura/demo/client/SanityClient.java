package de.tuberlin.aura.demo.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.MemoryManager;
import de.tuberlin.aura.core.statistic.MeasurementType;
import de.tuberlin.aura.core.statistic.NumberMeasurement;
import de.tuberlin.aura.core.task.common.*;
import de.tuberlin.aura.core.task.common.BenchmarkRecord.SanityBenchmarkRecord;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.AuraTopologyBuilder;
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

        private static final int RECORDS = 10;

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

                    LOG.debug("Source {} emit {}/{}", driverContext.taskDescriptor.taskIndex, index, outputs.size());

                    ++send;
                }

                LOG.debug("Source {}: {}/{}", driverContext.taskDescriptor.taskIndex, i, RECORDS);
            }

            LOG.debug("Emit completed");
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
    public static class Task2Exe extends TaskInvokeable {

        public Task2Exe(final TaskDriverContext context, DataProducer producer, final DataConsumer consumer, final Logger LOG) {
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

            LOG.debug("Output size: {}", outputs.size());

            while (!consumer.isExhausted() && isInvokeableRunning()) {
                final IOEvents.TransferBufferEvent event = consumer.absorb(0);

                LOG.debug("middle absorb");

                if (event != null) {
                    final Record<SanityBenchmarkRecord> record = driverContext.recordReader.readRecord(event);
                    ++bufferCount;

                    for (int index = 0; index < outputs.size(); ++index) {
                        final UUID outputTaskID = getTaskID(0, index);

                        final MemoryManager.MemoryView sendBuffer = producer.alloc();
                        final IOEvents.TransferBufferEvent outputBuffer = new IOEvents.TransferBufferEvent(taskID, outputTaskID, sendBuffer);

                        if (!record.getData().nextTask.equals(driverContext.taskDescriptor.taskID)) {
                            LOG.error("Buffer expected taskID: " + record.getData().nextTask.toString() + " but found "
                                    + driverContext.taskDescriptor.taskID + " Task: " + driverContext.taskDescriptor.name);
                        } else {
                            ++correct;
                        }

                        record.getData().nextTask = outputTaskID;

                        driverContext.recordWriter.writeRecord(record, outputBuffer);
                        producer.emit(0, index, outputBuffer);

                        LOG.debug("middle emit");
                    }

                    event.buffer.free();
                }
            }

            LOG.debug("middle emit completed");

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
    public static class Task3Exe extends TaskInvokeable {

        public Task3Exe(final TaskDriverContext context, DataProducer producer, final DataConsumer consumer, final Logger LOG) {
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

                LOG.debug("Sink {}: absorb {}", driverContext.taskDescriptor.taskIndex, receivedRecords);

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

        // Local
        // final String measurementPath = "/home/teots/Desktop/measurements";
        // final String zookeeperAddress = "localhost:2181";
        // final LocalClusterSimulator lce =
        // new
        // LocalClusterSimulator(LocalClusterSimulator.ExecutionMode.EXECUTION_MODE_SINGLE_PROCESS,
        // true,
        // zookeeperAddress,
        // 6,
        // measurementPath);

        // Wally
        final String zookeeperAddress = "wally001.cit.tu-berlin.de:2181";

        final AuraClient ac = new AuraClient(zookeeperAddress, 10000, 11111);

        final AuraTopologyBuilder atb1 = ac.createTopologyBuilder();
        atb1.addNode(new Node(UUID.randomUUID(), "Source", 264, 1), Source.class)
            .connectTo("Middle", Edge.TransferType.ALL_TO_ALL)
            .addNode(new Node(UUID.randomUUID(), "Middle", 264, 1), Task2Exe.class)
            .connectTo("Sink", Edge.TransferType.POINT_TO_POINT)
            .addNode(new Node(UUID.randomUUID(), "Sink", 264, 1), Task3Exe.class);
//        atb1.addNode(new Node(UUID.randomUUID(), "Source", 396, 1), Source.class)
//            .connectTo("Sink", Edge.TransferType.ALL_TO_ALL)
//            .addNode(new Node(UUID.randomUUID(), "Sink", 396, 1), Task3Exe.class);

        AuraTopology at;

        for (int i = 1; i <= 1; ++i) {
            at = atb1.build("Job " + Integer.toString(i), EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING));
            ac.submitTopology(at, null);

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

        // lce.shutdown();
    }
}
