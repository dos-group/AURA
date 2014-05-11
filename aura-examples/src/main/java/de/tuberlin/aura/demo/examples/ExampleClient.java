package de.tuberlin.aura.demo.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
import de.tuberlin.aura.core.task.common.DataConsumer;
import de.tuberlin.aura.core.task.common.DataProducer;
import de.tuberlin.aura.core.task.common.TaskDriverContext;
import de.tuberlin.aura.core.task.common.TaskInvokeable;
import de.tuberlin.aura.core.topology.AuraDirectedGraph;

public final class ExampleClient {

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(ExampleClient.class);

    // Disallow Instantiation.
    private ExampleClient() {}

    /**
     *
     */
    public static class Task1Exe extends TaskInvokeable {

        public Task1Exe(final TaskDriverContext driverContext, final DataProducer producer, final DataConsumer consumer, final Logger LOG) {

            super(driverContext, producer, consumer, LOG);
        }

        @Override
        public void run() throws Throwable {

            final UUID taskID = driverContext.taskDescriptor.taskID;

            int i = 0;
            int buffers = 1500;
            while (i++ < buffers && isInvokeableRunning()) {
                final List<Descriptors.TaskDescriptor> outputs = driverContext.taskBindingDescriptor.outputGateBindings.get(0);
                for (int index = 0; index < outputs.size(); ++index) {
                    final UUID outputTaskID = getTaskID(0, index);
                    final MemoryManager.MemoryView buffer = producer.alloc();
                    final IOEvents.DataIOEvent outputBuffer = new IOEvents.TransferBufferEvent(taskID, outputTaskID, buffer);
                    producer.emit(0, index, outputBuffer);
                }

                if (i % 1000 == 0) {
                    LOG.debug("Send {}/{}", i, buffers);
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

        public Task2Exe(final TaskDriverContext driverContext, final DataProducer producer, final DataConsumer consumer, final Logger LOG) {

            super(driverContext, producer, consumer, LOG);
        }

        @Override
        public void run() throws Throwable {

            final UUID taskID = driverContext.taskDescriptor.taskID;

            int i = 0;
            int buffers = 1000;
            while (i++ < buffers && isInvokeableRunning()) {
                final List<Descriptors.TaskDescriptor> outputs = driverContext.taskBindingDescriptor.outputGateBindings.get(0);
                for (int index = 0; index < outputs.size(); ++index) {
                    final UUID outputTaskID = getTaskID(0, index);
                    final MemoryManager.MemoryView buffer = producer.alloc();
                    final IOEvents.DataIOEvent outputBuffer = new IOEvents.TransferBufferEvent(taskID, outputTaskID, buffer);
                    producer.emit(0, index, outputBuffer);
                }

                if (i % 1000 == 0) {
                    LOG.debug("Send {}/{}", i, buffers);
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

        public Task3Exe(final TaskDriverContext driverContext, final DataProducer producer, final DataConsumer consumer, final Logger LOG) {

            super(driverContext, producer, consumer, LOG);
        }

        @Override
        public void open() throws Throwable {
            consumer.openGate(0);
            consumer.openGate(1);
        }

        @Override
        public void run() throws Throwable {

            int leftReceived = 0;
            int rightReceived = 0;
            int send = 0;
            while (!consumer.isExhausted() && isInvokeableRunning()) {

                final IOEvents.TransferBufferEvent left = consumer.absorb(0);
                final IOEvents.TransferBufferEvent right = consumer.absorb(1);

                if (left != null) {
                    // LOG.info("input left received data message from task " + left.srcTaskID);
                    left.buffer.free();
                    ++leftReceived;
                }

                if (right != null) {
                    // LOG.info("input right: received data message from task " + right.srcTaskID);
                    right.buffer.free();
                    ++rightReceived;
                }


                if (left != null || right != null) {
                    final List<Descriptors.TaskDescriptor> outputs = driverContext.taskBindingDescriptor.outputGateBindings.get(0);
                    for (int index = 0; index < outputs.size(); ++index) {
                        final UUID outputTaskID = getTaskID(0, index);
                        final MemoryManager.MemoryView buffer = producer.alloc();
                        final IOEvents.DataIOEvent outputBuffer =
                                new IOEvents.TransferBufferEvent(driverContext.taskDescriptor.taskID, outputTaskID, buffer);
                        producer.emit(0, index, outputBuffer);
                        ++send;
                    }

                    if (send % 1000 == 0) {
                        LOG.debug("Received (left: {}), (right: {}) and send {}", leftReceived, rightReceived, send);
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
    public static class Task4Exe extends TaskInvokeable {

        public Task4Exe(final TaskDriverContext driverContext, final DataProducer producer, final DataConsumer consumer, final Logger LOG) {

            super(driverContext, producer, consumer, LOG);
        }

        @Override
        public void open() throws Throwable {
            consumer.openGate(0);
        }

        @Override
        public void run() throws Throwable {
            int received = 0;
            while (!consumer.isExhausted() && isInvokeableRunning()) {
                final IOEvents.TransferBufferEvent buffer = consumer.absorb(0);
                // LOG.info("received: " + buffer);
                if (buffer != null) {
                    buffer.buffer.free();
                    ++received;
                }

                if (received % 1000 == 0) {
                    LOG.debug("Received {}", received);
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

        // Local
        // final String measurementPath = "/home/teots/Desktop/logs";
        // final String zookeeperAddress = "localhost:2181";
        // final LocalClusterSimulator lce =
        // new
        // LocalClusterSimulator(LocalClusterSimulator.ExecutionMode.EXECUTION_MODE_SINGLE_PROCESS,
        // true,
        // zookeeperAddress,
        // 4,
        // measurementPath);

        // Wally
        final String zookeeperAddress = "wally101.cit.tu-berlin.de:2181";

        final AuraClient ac = new AuraClient(zookeeperAddress, 10000, 11111);

        final AuraDirectedGraph.AuraTopologyBuilder atb1 = ac.createTopologyBuilder();
        atb1.addNode(new AuraDirectedGraph.Node(UUID.randomUUID(), "Task1", 198, 1), Task1Exe.class)
            .connectTo("Task3", AuraDirectedGraph.Edge.TransferType.ALL_TO_ALL)
            .addNode(new AuraDirectedGraph.Node(UUID.randomUUID(), "Task2", 198, 1), Task2Exe.class)
            .connectTo("Task3", AuraDirectedGraph.Edge.TransferType.ALL_TO_ALL)
            .addNode(new AuraDirectedGraph.Node(UUID.randomUUID(), "Task3", 198, 1), Task3Exe.class)
            .connectTo("Task4", AuraDirectedGraph.Edge.TransferType.POINT_TO_POINT)
            .addNode(new AuraDirectedGraph.Node(UUID.randomUUID(), "Task4", 198, 1), Task4Exe.class);

        // Add the job resubmission handler.
        ac.ioManager.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED, new EventHandler() {

            private int runs = 5;

            private int jobCounter = 1;

            @Handle(event = IOEvents.ControlIOEvent.class, type = IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED)
            private void handleTopologyFinished(final IOEvents.ControlIOEvent event) {
                String jobName = (String) event.getPayload();
                LOG.info("Topology ({}) finished.", jobName);

                if (jobCounter < runs) {
                    Thread t = new Thread() {

                        public void run() {
                            // This break is only necessary to make it easier to distinguish jobs in
                            // the log files.
                            try {
                                Thread.sleep(2000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                            AuraDirectedGraph.AuraTopology at =
                                    atb1.build("Job " + Integer.toString(++jobCounter),
                                               EnumSet.of(AuraDirectedGraph.AuraTopology.MonitoringType.NO_MONITORING));
                            ac.submitTopology(at, null);
                        }
                    };

                    t.start();
                }
            }
        });

        // Submit the first job. Other runs of this jobs are scheduled on demand.
        AuraDirectedGraph.AuraTopology at =
                atb1.build("Job " + Integer.toString(1), EnumSet.of(AuraDirectedGraph.AuraTopology.MonitoringType.NO_MONITORING));
        ac.submitTopology(at, null);

        try {
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        ac.closeSession();

        // lce.shutdown();
    }
}
