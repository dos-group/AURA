package de.tuberlin.aura.demo.client;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents.DataBufferEvent;
import de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent;
import de.tuberlin.aura.core.task.common.DataConsumer;
import de.tuberlin.aura.core.task.common.DataProducer;
import de.tuberlin.aura.core.task.common.TaskDriverContext;
import de.tuberlin.aura.core.task.common.TaskInvokeable;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.AuraTopologyBuilder;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.Edge;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.Node;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

public final class Client {

    private static final Logger LOG = Logger.getRootLogger();

    // Disallow Instantiation.
    private Client() {
    }

    /**
     *
     */
    public static class Task1Exe extends TaskInvokeable {

        public Task1Exe(final TaskDriverContext driverContext,
                        final DataProducer producer,
                        final DataConsumer consumer,
                        final Logger LOG) {

            super(driverContext, producer, consumer, LOG);
        }

        @Override
        public void run() throws Throwable {

            final UUID taskID = driverContext.taskDescriptor.taskID;

            for (int i = 0; i < 500; ++i) {
                final List<Descriptors.TaskDescriptor> outputs = driverContext.taskBindingDescriptor.outputGateBindings.get(0);
                for (int index = 0; index < outputs.size(); ++index) {
                    final UUID outputTaskID = getTaskID(0, index);
                    final DataIOEvent outputBuffer = new DataBufferEvent(taskID, outputTaskID, new byte[64 << 10]);
                    producer.emit(0, index, outputBuffer);
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
    public static class Task2Exe extends TaskInvokeable {

        public Task2Exe(final TaskDriverContext driverContext,
                        final DataProducer producer,
                        final DataConsumer consumer,
                        final Logger LOG) {

            super(driverContext, producer, consumer, LOG);
        }

        @Override
        public void run() throws Throwable {

            final UUID taskID = driverContext.taskDescriptor.taskID;

            for (int i = 0; i < 1000; ++i) {
                final List<Descriptors.TaskDescriptor> outputs = driverContext.taskBindingDescriptor.outputGateBindings.get(0);
                for (int index = 0; index < outputs.size(); ++index) {
                    final UUID outputTaskID = getTaskID(0, index);
                    final DataIOEvent outputBuffer = new DataBufferEvent(taskID, outputTaskID, new byte[64 << 10]);
                    producer.emit(0, index, outputBuffer);
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
    public static class Task3Exe extends TaskInvokeable {

        public Task3Exe(final TaskDriverContext driverContext,
                        final DataProducer producer,
                        final DataConsumer consumer,
                        final Logger LOG) {

            super(driverContext, producer, consumer, LOG);
        }

        @Override
        public void open() throws Throwable {
            consumer.openGate(0);
            consumer.openGate(1);
        }

        @Override
        public void run() throws Throwable {

            int count = 0;

            while (!consumer.isExhausted()) {

                final DataIOEvent left = consumer.absorb(0);
                final DataIOEvent right = consumer.absorb(1);

                //if (left != null)
                //    LOG.info("input left received data message from task " + left.srcTaskID);
                //if (right != null)
                //    LOG.info("input right: received data message from task " + right.srcTaskID);

                if (left != null || right != null) {

                    final List<Descriptors.TaskDescriptor> outputs = driverContext.taskBindingDescriptor.outputGateBindings.get(0);
                    for (int index = 0; index < outputs.size(); ++index) {

                        final UUID outputTaskID = getTaskID(0, index);
                        final DataIOEvent outputBuffer = new DataBufferEvent(driverContext.taskDescriptor.taskID, outputTaskID, new byte[64 << 10]);
                        producer.emit(0, index, outputBuffer);
                    }

                    if (count == 150)
                        throw new IllegalStateException();

                    count++;
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
    public static class Task4Exe extends TaskInvokeable {

        public Task4Exe(final TaskDriverContext driverContext,
                        final DataProducer producer,
                        final DataConsumer consumer,
                        final Logger LOG) {

            super(driverContext, producer, consumer, LOG);
        }

        @Override
        public void open() throws Throwable {
            consumer.openGate(0);
        }

        @Override
        public void run() throws Throwable {

            while (!consumer.isExhausted()) {

                final DataIOEvent input = consumer.absorb(0);

                if (input != null) {
                    //LOG.info("- received");
                }
            }
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
        final LocalClusterSimulator lce = new LocalClusterSimulator(LocalClusterSimulator.ExecutionMode.EXECUTION_MODE_SINGLE_PROCESS, true, zookeeperAddress, 8);
        final AuraClient ac = new AuraClient(zookeeperAddress, 25340, 26340);

        final AuraTopologyBuilder atb1 = ac.createTopologyBuilder();

        atb1.addNode(new Node(UUID.randomUUID(), "Task1", 2, 1), Task1Exe.class)
                .connectTo("Task3", Edge.TransferType.ALL_TO_ALL)
                .addNode(new Node(UUID.randomUUID(), "Task2", 2, 1), Task2Exe.class)
                .connectTo("Task3", Edge.TransferType.ALL_TO_ALL)
                .addNode(new Node(UUID.randomUUID(), "Task3", 2, 1), Task3Exe.class)
                .connectTo("Task4", Edge.TransferType.POINT_TO_POINT)
                .addNode(new Node(UUID.randomUUID(), "Task4", 2, 1), Task4Exe.class);

        final AuraTopology at1 = atb1.build("Job 1", EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING));

        ac.submitTopology(at1, null);

        try {
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        lce.shutdown();
    }
}
