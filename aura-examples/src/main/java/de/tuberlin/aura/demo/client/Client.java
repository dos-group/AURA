package de.tuberlin.aura.demo.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterExecutor;
import de.tuberlin.aura.client.executors.LocalClusterExecutor.LocalExecutionMode;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.IOEvents.DataBufferEvent;
import de.tuberlin.aura.core.iosystem.IOEvents.DataEventType;
import de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent;
import de.tuberlin.aura.core.task.common.TaskInvokeable;
import de.tuberlin.aura.core.task.common.TaskRuntimeContext;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.AuraTopologyBuilder;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.Edge;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.Node;

public final class Client {

    private static final Logger LOG = Logger.getRootLogger();

    // Disallow Instantiation.
    private Client() {}

    /**
     *
     */
    public static class Task1Exe extends TaskInvokeable {

        public Task1Exe(final TaskRuntimeContext context, final Logger LOG) {
            super(context, LOG);
        }

        @Override
        public void execute() throws Exception {

            // openGate(0);

            final UUID taskID = getTaskID();

            for (int i = 0; i < 100; ++i) {

                final List<Descriptors.TaskDescriptor> outputs = context.taskBinding.outputGateBindings.get(0);
                for (int index = 0; index < outputs.size(); ++index) {
                    final UUID outputTaskID = getOutputTaskID(0, index);

                    ByteBuffer buffer = ByteBuffer.allocate(64 << 10);
                    buffer.putInt(i);
                    buffer.flip();

                    final DataIOEvent outputBuffer = new DataBufferEvent(taskID, outputTaskID, buffer.array());
                    emit(0, index, outputBuffer);
                    LOG.error("---> 1 emit");
                }

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    LOG.error(e);
                }
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

        public Task2Exe(final TaskRuntimeContext context, final Logger LOG) {
            super(context, LOG);
        }

        @Override
        public void execute() throws Exception {

            final UUID taskID = getTaskID();

            for (int i = 0; i < 100; ++i) {

                final List<Descriptors.TaskDescriptor> outputs = context.taskBinding.outputGateBindings.get(0);
                for (int index = 0; index < outputs.size(); ++index) {
                    final UUID outputTaskID = getOutputTaskID(0, index);
                    final DataIOEvent outputBuffer = new DataBufferEvent(taskID, outputTaskID, new byte[64 << 10]);
                    emit(0, index, outputBuffer);
                }

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    LOG.error(e);
                }
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
    public static class Task33Exe extends TaskInvokeable {

        public Task33Exe(final TaskRuntimeContext context, final Logger LOG) {
            super(context, LOG);
        }

        @Override
        public void execute() throws Exception {

            final UUID taskID = getTaskID();

            openGate(0);

            while (isTaskRunning()) {

                final DataIOEvent leftInputBuffer = absorb(0);

                if (leftInputBuffer != null)
                    LOG.info("[" + getTaskIndex() + "] inner " + leftInputBuffer.srcTaskID);

                if (leftInputBuffer instanceof DataBufferEvent) {

                    int received = ByteBuffer.wrap(((DataBufferEvent) leftInputBuffer).data).getInt();
                    LOG.error("- inner:" + received);
                }

                if (!DataEventType.DATA_EVENT_SOURCE_EXHAUSTED.equals(leftInputBuffer == null ? null : leftInputBuffer.type)) {
                    final List<Descriptors.TaskDescriptor> outputs = context.taskBinding.outputGateBindings.get(0);
                    for (int index = 0; index < outputs.size(); ++index) {

                        final UUID outputTaskID = getOutputTaskID(0, index);
                        final DataIOEvent outputBuffer = new DataBufferEvent(taskID, outputTaskID, ((DataBufferEvent) leftInputBuffer).data);
                        emit(0, index, outputBuffer);
                    }
                }

                checkIfSuspended();
            }

            final List<Descriptors.TaskDescriptor> outputs = context.taskBinding.outputGateBindings.get(0);
            for (int index = 0; index < outputs.size(); ++index) {
                final UUID outputTaskID = getOutputTaskID(0, index);
                final DataIOEvent exhaustedEvent = new DataIOEvent(DataEventType.DATA_EVENT_SOURCE_EXHAUSTED, taskID, outputTaskID);
                emit(0, index, exhaustedEvent);
            }

            // closeGate(0);
            // closeGate(1);
        }
    }

    /**
     *
     */
    public static class Task3Exe extends TaskInvokeable {

        public Task3Exe(final TaskRuntimeContext context, final Logger LOG) {
            super(context, LOG);
        }

        @Override
        public void execute() throws Exception {

            final UUID taskID = getTaskID();

            openGate(0);
            // openGate(1);

            while (isTaskRunning()) {

                final DataIOEvent leftInputBuffer = absorb(0);
                final DataIOEvent rightInputBuffer = absorb(1);

                if (leftInputBuffer != null)
                    LOG.info("[" + getTaskIndex() + "] input left: received data message from task " + leftInputBuffer.srcTaskID);

                if (rightInputBuffer != null)
                    LOG.info("[" + getTaskIndex() + "] input right: received data message from task " + rightInputBuffer.srcTaskID);

                if (!DataEventType.DATA_EVENT_SOURCE_EXHAUSTED.equals(leftInputBuffer == null ? null : leftInputBuffer.type)
                        || !DataEventType.DATA_EVENT_SOURCE_EXHAUSTED.equals(rightInputBuffer == null ? null : rightInputBuffer.type)) {
                    final List<Descriptors.TaskDescriptor> outputs = context.taskBinding.outputGateBindings.get(0);
                    for (int index = 0; index < outputs.size(); ++index) {

                        final UUID outputTaskID = getOutputTaskID(0, index);
                        final DataIOEvent outputBuffer = new DataBufferEvent(taskID, outputTaskID, new byte[64 << 10]);
                        emit(0, index, outputBuffer);
                    }
                }

                checkIfSuspended();
            }

            final List<Descriptors.TaskDescriptor> outputs = context.taskBinding.outputGateBindings.get(0);
            for (int index = 0; index < outputs.size(); ++index) {
                final UUID outputTaskID = getOutputTaskID(0, index);
                final DataIOEvent exhaustedEvent = new DataIOEvent(DataEventType.DATA_EVENT_SOURCE_EXHAUSTED, taskID, outputTaskID);
                emit(0, index, exhaustedEvent);
            }

            // closeGate(0);
            // closeGate(1);
        }
    }

    /**
     *
     */
    public static class Task4Exe extends TaskInvokeable {

        int count = 0;

        int sum_received = 0;

        int sum_count = 0;

        public Task4Exe(final TaskRuntimeContext context, final Logger LOG) {
            super(context, LOG);
        }

        @Override
        public void execute() throws Exception {

            openGate(0);

            // boolean inputActive = true;

            while (isTaskRunning()) {

                final DataIOEvent inputBuffer = absorb(0);

                if (inputBuffer instanceof DataBufferEvent) {

                    int received = ByteBuffer.wrap(((DataBufferEvent) inputBuffer).data).getInt();
                    LOG.error("- received: " + received + " - count: " + count);


                    sum_received += received;
                    sum_count += count;

                    count++;
                }



                // LOG.info("received data message from task " + inputBuffer.srcTaskID + " // " +
                // inputBuffer.toString());

                if (count == 10) {
                    closeGate(0);
                    Thread.sleep(10000);
                    openGate(0);
                }

                // inputActive =
                // !DataEventType.DATA_EVENT_SOURCE_EXHAUSTED.equals(inputBuffer.type);

                checkIfSuspended();
            }

            LOG.error("received sum: " + sum_received + " -- count sum: " + sum_count);

            LOG.info("RECEIVED ELEMENTS: " + count);

      //closeGate(0);
        }
    }

    // ---------------------------------------------------
    // Main.
    // ---------------------------------------------------

    public static void main(String[] args) {

        // final SimpleLayout layout = new SimpleLayout();
        // final ConsoleAppender consoleAppender = new ConsoleAppender(layout);
        // LOG.addAppender(consoleAppender);
        // LOG.setLevel(Level.DEBUG);

        final String zookeeperAddress = "localhost:2181";
        final LocalClusterExecutor lce = new LocalClusterExecutor(LocalExecutionMode.EXECUTION_MODE_SINGLE_PROCESS, true, zookeeperAddress, 4);
        final AuraClient ac = new AuraClient(zookeeperAddress, 25340, 26340);

        final AuraTopologyBuilder atb1 = ac.createTopologyBuilder();
        // atb1.addNode(new Node(UUID.randomUUID(), "Task1", 2, 1), Task1Exe.class)
        // .connectTo("Task3", Edge.TransferType.ALL_TO_ALL)
        // .addNode(new Node(UUID.randomUUID(), "Task2", 3, 1), Task2Exe.class)
        // .connectTo("Task3", Edge.TransferType.ALL_TO_ALL)
        // .addNode(new Node(UUID.randomUUID(), "Task3", 2, 1), Task3Exe.class)
        // .connectTo("Task4", Edge.TransferType.POINT_TO_POINT)
        // .addNode(new Node(UUID.randomUUID(), "Task4", 4, 1), Task4Exe.class);

        atb1.addNode(new Node(UUID.randomUUID(), "Task1", 2, 1), Task1Exe.class)
            .connectTo("Task3", Edge.TransferType.ALL_TO_ALL)
            .addNode(new Node(UUID.randomUUID(), "Task2", 3, 1), Task2Exe.class)
            .connectTo("Task3", Edge.TransferType.ALL_TO_ALL)
            .addNode(new Node(UUID.randomUUID(), "Task3", 2, 1), Task3Exe.class)
            .connectTo("Task4", Edge.TransferType.POINT_TO_POINT)
            .addNode(new Node(UUID.randomUUID(), "Task4", 4, 1), Task4Exe.class);

        final AuraTopologyBuilder atb2 = ac.createTopologyBuilder();
        atb2.addNode(new Node(UUID.randomUUID(), "Task1", 1, 1), Task1Exe.class)
            .connectTo("Task33", Edge.TransferType.POINT_TO_POINT)
            .addNode(new Node(UUID.randomUUID(), "Task33", 1, 1), Task33Exe.class)
            .connectTo("Task4", Edge.TransferType.POINT_TO_POINT)
            .addNode(new Node(UUID.randomUUID(), "Task4", 1, 1), Task4Exe.class);

        final AuraTopology at1 = atb1.build("Job 1", EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING));

        final AuraTopology at2 = atb2.build("Job 2", EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING));

        final EventHandler monitoringHandler = new EventHandler() {

            @Handle(event = IOEvents.MonitoringEvent.class, type = IOEvents.MonitoringEvent.MONITORING_TOPOLOGY_STATE_EVENT)
            private void handleMonitoredTopologyEvent(final IOEvents.MonitoringEvent event) {
                LOG.info(event.type + ": " + event.topologyStateUpdate.currentTopologyState.toString() + " ---- "
                        + event.topologyStateUpdate.topologyTransition.toString() + " ----> "
                        + event.topologyStateUpdate.nextTopologyState.toString() + " - " + event.topologyStateUpdate.currentTopologyState.toString()
                        + " duration (" + event.topologyStateUpdate.stateDuration + "ms)");
            }

            @Handle(event = IOEvents.MonitoringEvent.class, type = IOEvents.MonitoringEvent.MONITORING_TASK_STATE_EVENT)
            private void handleMonitoredTaskEvent(final IOEvents.MonitoringEvent event) {
                LOG.info(event.type + ": " + event.taskStateUpdate.currentTaskState.toString() + " ---- "
                        + event.taskStateUpdate.taskTransition.toString() + " ----> " + event.taskStateUpdate.nextTaskState.toString() + " - "
                        + event.taskStateUpdate.currentTaskState.toString() + " duration (" + event.taskStateUpdate.stateDuration + "ms)");
            }
        };

        // ac.submitTopology(at1, monitoringHandler);
        ac.submitTopology(at2, null);

        try {
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        lce.shutdown();
    }
}
