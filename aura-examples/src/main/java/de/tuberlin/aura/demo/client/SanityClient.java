package de.tuberlin.aura.demo.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
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

    private static final Logger LOG = Logger.getRootLogger();

    // Disallow Instantiation.
    private SanityClient() {}

    /**
*
*/
    public static class Source extends TaskInvokeable {

        private static final int RECORDS = 10000;

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
                    final IOEvents.TransferBufferEvent outputBuffer = new IOEvents.TransferBufferEvent(taskID, outputTaskID, buffer);

                    final Record<SanityBenchmarkRecord> record = new Record<>(new SanityBenchmarkRecord(outputTaskID));

                    driverContext.recordWriter.writeRecord(record, outputBuffer);
                    producer.emit(0, index, outputBuffer);

                    ++send;
                }
            }

            driverContext.measurementManager.add(new NumberMeasurement(MeasurementType.NUMBER, "SOURCE", send));
            // LOG.info("SOURCE|" + Integer.toString(send));
        }

        @Override
        public void close() throws Throwable {
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
            long bufferCount = 0l;
            long correct = 0l;

            while (!consumer.isExhausted() && isInvokeableRunning()) {
                final IOEvents.TransferBufferEvent buffer = consumer.absorb(0);
                if (buffer != null) {
                    final Record<SanityBenchmarkRecord> record = driverContext.recordReader.readRecord(buffer);
                    final List<Descriptors.TaskDescriptor> outputs = driverContext.taskBindingDescriptor.outputGateBindings.get(0);
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
                    }

                    buffer.buffer.free();
                }
            }

            driverContext.measurementManager.add(new NumberMeasurement(MeasurementType.NUMBER, "BUFFERS", bufferCount));
            driverContext.measurementManager.add(new NumberMeasurement(MeasurementType.NUMBER, "CORRECT_BUFFERS", correct));
        }

        @Override
        public void close() throws Throwable {
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
                final IOEvents.TransferBufferEvent buffer = consumer.absorb(0);

                if (buffer != null) {
                    final Record<SanityBenchmarkRecord> record = driverContext.recordReader.readRecord(buffer);
                    if (!record.getData().nextTask.equals(driverContext.taskDescriptor.taskID)) {
                        LOG.error("Buffer expected taskID: " + record.getData().nextTask.toString() + " but found "
                                + driverContext.taskDescriptor.taskID + " Task: " + driverContext.taskDescriptor.name);
                    }

                    ++receivedRecords;
                    buffer.buffer.free();
                }
            }

            driverContext.measurementManager.add(new NumberMeasurement(MeasurementType.NUMBER, "SINK", receivedRecords));
            // LOG.info("SINK|" + Integer.toString(receivedRecords));
        }
    }


    // ---------------------------------------------------
    // Main.
    // ---------------------------------------------------

    public static void main(String[] args) {

        final SimpleLayout layout = new SimpleLayout();
        final ConsoleAppender consoleAppender = new ConsoleAppender(layout);
        LOG.addAppender(consoleAppender);
        LOG.setLevel(Level.INFO);

        final String measurementPath = "/home/teots/Desktop/measurements";
        // final String zookeeperAddress = "wally033.cit.tu-berlin.de:2181";
        final String zookeeperAddress = "localhost:2181";
        final LocalClusterSimulator lce =
                new LocalClusterSimulator(LocalClusterSimulator.ExecutionMode.EXECUTION_MODE_SINGLE_PROCESS,
                                          true,
                                          zookeeperAddress,
                                          6,
                                          measurementPath);
        final AuraClient ac = new AuraClient(zookeeperAddress, 10000, 11111);

        final AuraTopologyBuilder atb1 = ac.createTopologyBuilder();
        atb1.addNode(new Node(UUID.randomUUID(), "Task1", 2, 1), Source.class)
            .connectTo("Task2", Edge.TransferType.ALL_TO_ALL)
            .addNode(new Node(UUID.randomUUID(), "Task2", 2, 1), Task2Exe.class)
            .connectTo("Task3", Edge.TransferType.POINT_TO_POINT)
            .addNode(new Node(UUID.randomUUID(), "Task3", 2, 1), Task3Exe.class);

        AuraTopology at;

        for (int i = 1; i <= 1; ++i) {
            at = atb1.build("Job " + Integer.toString(i), EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING));
            ac.submitTopology(at, null);

            try {
                Thread.sleep(30000);
            } catch (InterruptedException e) {
                LOG.error(e);
            }
        }

        try {
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        lce.shutdown();
    }
}
