package de.tuberlin.aura.demo.client;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents.DataBufferEvent;
import de.tuberlin.aura.core.iosystem.IOEvents.DataEventType;
import de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent;
import de.tuberlin.aura.core.task.common.BenchmarkRecord.SanityBenchmarkRecord;
import de.tuberlin.aura.core.task.common.Record;
import de.tuberlin.aura.core.task.common.TaskInvokeable;
import de.tuberlin.aura.core.task.common.TaskRuntimeContext;
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
import java.util.List;
import java.util.UUID;

public final class SanityClient {

    private static final Logger LOG = Logger.getRootLogger();

    // Disallow Instantiation.
    private SanityClient() {
    }

    /**
     *
     */
    public static class Source extends TaskInvokeable {

        private static final int BUFFER_SIZE = 64000;

        private static final int RECORDS = 10000;

        public Source(final TaskRuntimeContext context, final Logger LOG) {
            super(context, LOG);
        }

        @Override
        public void execute() throws Exception {

            final UUID taskID = getTaskID();

            int send = 0;
            for (int i = 0; i < RECORDS; ++i) {

                final List<Descriptors.TaskDescriptor> outputs = context.taskBinding.outputGateBindings.get(0);
                for (int index = 0; index < outputs.size(); ++index) {
                    final UUID outputTaskID = getOutputTaskID(0, index);
                    final DataBufferEvent outputBuffer = new DataBufferEvent(taskID, outputTaskID, new byte[BUFFER_SIZE]);
                    final Record<SanityBenchmarkRecord> record = new Record<>(new SanityBenchmarkRecord(outputTaskID));

                    emit(0, index, record, outputBuffer);
                    ++send;
                }

//                try {
//                    Thread.sleep(100);
//                } catch (InterruptedException e) {
//                    LOG.error(e);
//                }
            }

            LOG.info("SOURCE|" + Integer.toString(send));

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

            int correct = 0;
            int bufferCount = 0;

            while (isTaskRunning()) {
                final List<Descriptors.TaskDescriptor> outputs = context.taskBinding.outputGateBindings.get(0);
                final Record<SanityBenchmarkRecord> record = absorb(0);
                if (record != null) {

                    ++bufferCount;

                    for (int index = 0; index < outputs.size(); ++index) {
                        final UUID outputTaskID = getOutputTaskID(0, index);
                        final DataBufferEvent outputBuffer = new DataBufferEvent(taskID, outputTaskID, new byte[BUFFER_SIZE]);

                        if (!record.getData().nextTask.equals(getTaskID())) {
                            LOG.error("Buffer expected taskID: " + record.getData().nextTask.toString()
                                    + " but found " + getTaskID() + " Task: " + getTaskName());
                        } else {
                            ++correct;
                        }

                        record.getData().nextTask = outputTaskID;
                        emit(0, index, record, outputBuffer);
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

            LOG.error("Buffers: " + Integer.toString(bufferCount));
            LOG.error("Correct buffers: " + Integer.toString(correct));

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

            openGate(0);

            int receivedRecords = 0;

            while (isTaskRunning()) {

                final Record<SanityBenchmarkRecord> record = absorb(0);

                if (record != null) {
                    if (!record.getData().nextTask.equals(getTaskID())) {
                        LOG.error("Buffer expected taskID: " + record.getData().nextTask.toString()
                                + " but found " + getTaskID() + " Task: " + getTaskName());
                    }

                    ++receivedRecords;
                }

                checkIfSuspended();
            }

            LOG.info("SINK|" + Integer.toString(receivedRecords));

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
        // final String zookeeperAddress = "localhost:2181";
        // final LocalClusterExecutor lce = new LocalClusterExecutor(LocalClusterExecutor.LocalExecutionMode.EXECUTION_MODE_SINGLE_PROCESS, true, zookeeperAddress, 6);
        final AuraClient ac = new AuraClient(zookeeperAddress, 10000, 11111);

        final AuraTopologyBuilder atb1 = ac.createTopologyBuilder();
        atb1.addNode(new Node(UUID.randomUUID(), "Task1", 2, 1), Source.class)
                .connectTo("Task2", Edge.TransferType.ALL_TO_ALL)
                .addNode(new Node(UUID.randomUUID(), "Task2", 2, 1), Task2Exe.class)
                .connectTo("Task3", Edge.TransferType.POINT_TO_POINT)
                .addNode(new Node(UUID.randomUUID(), "Task3", 2, 1), Task3Exe.class);

        AuraTopology at;

        for (int i = 1; i <= 10; ++i) {
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

        //lce.shutdown();
    }
}
