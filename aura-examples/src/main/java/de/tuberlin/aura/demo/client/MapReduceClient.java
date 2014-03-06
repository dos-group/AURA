package de.tuberlin.aura.demo.client;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents.DataBufferEvent;
import de.tuberlin.aura.core.iosystem.IOEvents.DataEventType;
import de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent;
import de.tuberlin.aura.core.task.common.*;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.AuraTopologyBuilder;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.Edge;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.Node;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public final class MapReduceClient {

    private static final Logger LOG = Logger.getRootLogger();

    // Disallow Instantiation.
    private MapReduceClient() {
    }

    /**
     *
     */
    public static class Source extends TaskInvokeable {

        private static final int BUFFER_SIZE = 64000;

        private static final int RECORDS = 1000;

        public Source(final TaskRuntimeContext context, final Logger LOG) {
            super(context, LOG);
        }

        @Override
        public void execute() throws Exception {

            final UUID taskID = getTaskID();

            for (int i = 0; i < RECORDS; ++i) {

                final List<Descriptors.TaskDescriptor> outputs = context.taskBinding.outputGateBindings.get(0);
                for (int index = 0; index < outputs.size(); ++index) {
                    final UUID outputTaskID = getOutputTaskID(0, index);
                    final DataBufferEvent outputBuffer = new DataBufferEvent(taskID, outputTaskID, new byte[BUFFER_SIZE]);
                    final Record<BenchmarkRecord> record = new Record<>(new BenchmarkRecord());

                    emit(0, index, record, outputBuffer);

                    LOG.info("Emit at: " + record.getData().time);
                }

//                try {
//                    Thread.sleep(100);
//                } catch (InterruptedException e) {
//                    LOG.error(e);
//                }
            }

            final List<Descriptors.TaskDescriptor> outputs = context.taskBinding.outputGateBindings.get(0);
            for (int index = 0; index < outputs.size(); ++index) {
                final UUID outputTaskID = getOutputTaskID(0, index);
                final DataIOEvent exhaustedEvent = new DataIOEvent(DataEventType.DATA_EVENT_SOURCE_EXHAUSTED, taskID, outputTaskID);
                emit(0, index, exhaustedEvent);
            }
        }
    }

    /**
     *
     */
    public static class Task2Exe extends TaskInvokeable {

        private static final int BUFFER_SIZE = 64000;

        public Task2Exe(final TaskRuntimeContext context, final Logger LOG) {
            super(context, LOG);
        }

        @Override
        public void execute() throws Exception {

            final UUID taskID = getTaskID();

            openGate(0);

            while (isTaskRunning()) {
                final List<Descriptors.TaskDescriptor> outputs = context.taskBinding.outputGateBindings.get(0);
                final Record<BenchmarkRecord> record = absorb(0);
                if (record != null) {
                    for (int index = 0; index < outputs.size(); ++index) {
                        final UUID outputTaskID = getOutputTaskID(0, index);
                        final DataBufferEvent outputBuffer = new DataBufferEvent(taskID, outputTaskID, new byte[BUFFER_SIZE]);
                        emit(0, index, record, outputBuffer);

                        LOG.info("Emit at: " + record.getData().time);
                    }
                } else {
                    for (int index = 0; index < outputs.size(); ++index) {
                        final UUID outputTaskID = getOutputTaskID(0, index);
                        final DataIOEvent exhaustedEvent = new DataIOEvent(DataEventType.DATA_EVENT_SOURCE_EXHAUSTED, taskID, outputTaskID);
                        emit(0, index, exhaustedEvent);
                    }
                }

                checkIfSuspended();
            }

            closeGate(0);
        }
    }

    /**
     *
     */
    public static class Task3Exe extends TaskInvokeable {

        private static final int BUFFER_SIZE = 64000;

        public Task3Exe(final TaskRuntimeContext context, final Logger LOG) {
            super(context, LOG);
        }

        @Override
        public void execute() throws Exception {

            final UUID taskID = getTaskID();

            openGate(0);
            openGate(1);

            while (isTaskRunning()) {

                final Record<BenchmarkRecord> leftRecord = absorb(0);
                final Record<BenchmarkRecord> rightRecord = absorb(1);

                if (leftRecord != null && rightRecord != null) {
                    final List<Descriptors.TaskDescriptor> outputs = context.taskBinding.outputGateBindings.get(0);
                    for (int index = 0; index < outputs.size(); ++index) {

                        final UUID outputTaskID = getOutputTaskID(0, index);

                        final DataBufferEvent leftOutputBuffer = new DataBufferEvent(taskID, outputTaskID, new byte[BUFFER_SIZE]);
                        emit(0, index, leftRecord, leftOutputBuffer);

                        final DataBufferEvent rightOutputBuffer = new DataBufferEvent(taskID, outputTaskID, new byte[BUFFER_SIZE]);
                        emit(0, index, rightRecord, rightOutputBuffer);
                    }
                }

                checkIfSuspended();
            }

            closeGate(0);
            closeGate(1);
        }
    }

    /**
     *
     */
    public static class Task4Exe extends TaskInvokeable {

        private static final int BUFFER_SIZE = 64000;

        public Task4Exe(final TaskRuntimeContext context, final Logger LOG) {
            super(context, LOG);
        }

        @Override
        public void execute() throws Exception {

            final UUID taskID = getTaskID();

            openGate(0);

            LinkedList<Long> latencies = new LinkedList<>();

            while (isTaskRunning()) {

                final Record<BenchmarkRecord> record = absorb(0);
                if (record != null) {
                    long latency = System.currentTimeMillis() - record.getData().time;
                    latencies.add(latency);
                }

                checkIfSuspended();
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

            LOG.info("RESULTS|" + Double.toString(avgLatency) + "|" + Long.toString(minLatency) + "|"
                    + Long.toString(maxLatency) + "|" + Long.toString(medianLatency));

            closeGate(0);
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

        final String zookeeperAddress = "wally033.cit.tu-berlin.de:2181";
        //final LocalClusterExecutor lce = new LocalClusterExecutor(LocalExecutionMode.EXECUTION_MODE_SINGLE_PROCESS, true, zookeeperAddress, 4);
        final AuraClient ac = new AuraClient(zookeeperAddress, 10000, 11111);

        final AuraTopologyBuilder atb1 = ac.createTopologyBuilder();
        atb1.addNode(new Node(UUID.randomUUID(), "Task1", 1, 1), Source.class)
                .connectTo("Task2", Edge.TransferType.ALL_TO_ALL)
                .addNode(new Node(UUID.randomUUID(), "Task2", 2, 1), Task2Exe.class)
                .connectTo("Task3", Edge.TransferType.POINT_TO_POINT)
                .addNode(new Node(UUID.randomUUID(), "Task3", 2, 1), Task4Exe.class);


        final AuraTopology at1 = atb1.build("Job 1", EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING));

        for (int i = 0; i < 10; ++i) {
            ac.submitTopology(at1, null);

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

        // lce.shutdown();
    }
}
