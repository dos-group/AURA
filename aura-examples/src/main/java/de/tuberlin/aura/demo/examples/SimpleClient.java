package de.tuberlin.aura.demo.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.memory.test.BufferAllocator;
import de.tuberlin.aura.core.task.spi.*;
import de.tuberlin.aura.taskmanager.TaskRecordReader;
import de.tuberlin.aura.taskmanager.TaskRecordWriter;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.SimpleLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.topology.AuraDirectedGraph;

public final class SimpleClient {

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(SimpleClient.class);

    // Disallow Instantiation.
    private SimpleClient() {}

    /**
     *
     */
    public static class Task1Exe extends AbstractTaskInvokeable {

        public Task1Exe(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {

            super(taskDriver, producer, consumer, LOG);
        }

        @Override
        public void run() throws Throwable {

            final UUID taskID = taskDriver.getTaskDescriptor().taskID;
            int i = 0;
            while (i++ < 1 && isInvokeableRunning()) {
                final List<Descriptors.TaskDescriptor> outputs = taskDriver.getTaskBindingDescriptor().outputGateBindings.get(0);
                for (int index = 0; index < outputs.size(); ++index) {
                    final UUID outputTaskID = getTaskID(0, index);
                    final MemoryView buffer = producer.alloc();
                    final IOEvents.DataIOEvent outputBuffer = new IOEvents.TransferBufferEvent(taskID, outputTaskID, buffer);
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
    /*public static class Task2Exe extends AbstractTaskInvokeable {

        public Task2Exe(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {

            super(taskDriver, producer, consumer, LOG);
        }

        @Override
        public void run() throws Throwable {

            final UUID taskID = taskDriver.getTaskDescriptor().taskID;

            int i = 0;
            int buffers = 1000;
            while (i++ < buffers && isInvokeableRunning()) {
                final List<Descriptors.TaskDescriptor> outputs = taskDriver.getTaskBindingDescriptor().outputGateBindings.get(0);
                for (int index = 0; index < outputs.size(); ++index) {
                    final UUID outputTaskID = getTaskID(0, index);
                    final MemoryView buffer = producer.alloc();
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
    }*/

    /**
     *
     */
    public static class Task3Exe extends AbstractTaskInvokeable {

        private final IRecordWriter recordWriter;

        public Task3Exe(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {

            super(taskDriver, producer, consumer, LOG);

            this.recordWriter = new TaskRecordWriter(BufferAllocator._64K);
        }

        @Override
        public void open() throws Throwable {
            consumer.openGate(0);
        }

        @Override
        public void run() throws Throwable {

            while (!consumer.isExhausted() && isInvokeableRunning()) {

                final IOEvents.TransferBufferEvent input = consumer.absorb(0);

                if (input != null) {
                    input.buffer.free();
                }

                if (input != null) {
                    final List<Descriptors.TaskDescriptor> outputs = taskDriver.getTaskBindingDescriptor().outputGateBindings.get(0);
                    for (int index = 0; index < outputs.size(); ++index) {
                        final UUID outputTaskID = getTaskID(0, index);
                        final MemoryView buffer = producer.alloc();

                        final IOEvents.TransferBufferEvent outputBuffer =
                                new IOEvents.TransferBufferEvent(taskDriver.getTaskDescriptor().taskID, outputTaskID, buffer);

                        this.recordWriter.selectBuffer(outputBuffer);

                        this.recordWriter.writeRecord(new Integer(101));

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
    public static class Task4Exe extends AbstractTaskInvokeable {

        private IRecordReader recordReader;

        public Task4Exe(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {

            super(taskDriver, producer, consumer, LOG);

            this.recordReader = new TaskRecordReader(BufferAllocator._64K);
        }

        @Override
        public void open() throws Throwable {
            consumer.openGate(0);
        }

        @Override
        public void run() throws Throwable {

            int bufferCounter = 0;

            while (!consumer.isExhausted() && isInvokeableRunning()) {
                final IOEvents.TransferBufferEvent buffer = consumer.absorb(0);

                if(buffer != null) {
                    recordReader.selectBuffer(buffer);
                    final Integer value = recordReader.readRecord(Integer.class);
                    LOG.info("Value = " + value);
                }

                if (buffer != null) {
                    buffer.buffer.free();
                    ++bufferCounter;
                }
            }
        }
    }

    // ---------------------------------------------------
    // Main.
    // ---------------------------------------------------

    public static void main(String[] args) {

        final SimpleLayout layout = new SimpleLayout();
        final ConsoleAppender consoleAppender = new ConsoleAppender(layout);

        // Local
        final String measurementPath = "/home/tobias/Desktop/logs";
        final String zookeeperAddress = "localhost:2181";
        final LocalClusterSimulator lce =
        new LocalClusterSimulator(LocalClusterSimulator.ExecutionMode.EXECUTION_MODE_SINGLE_PROCESS,
        true,
        zookeeperAddress,
        4,
        measurementPath);

        // Wally
        //final String zookeeperAddress = "wally101.cit.tu-berlin.de:2181";

        final AuraClient ac = new AuraClient(zookeeperAddress, 10000, 11111);

        final AuraDirectedGraph.AuraTopologyBuilder atb1 = ac.createTopologyBuilder();
        atb1.addNode(new AuraDirectedGraph.Node(UUID.randomUUID(), "Task1", 4, 1), Task1Exe.class)
            .connectTo("Task3", AuraDirectedGraph.Edge.TransferType.ALL_TO_ALL)
            //.addNode(new AuraDirectedGraph.Node(UUID.randomUUID(), "Task2", 4, 1), Task2Exe.class)
            //.connectTo("Task3", AuraDirectedGraph.Edge.TransferType.ALL_TO_ALL)
            .addNode(new AuraDirectedGraph.Node(UUID.randomUUID(), "Task3", 4, 1), Task3Exe.class)
            .connectTo("Task4", AuraDirectedGraph.Edge.TransferType.ALL_TO_ALL)
            .addNode(new AuraDirectedGraph.Node(UUID.randomUUID(), "Task4", 4, 1), Task4Exe.class);

        ac.submitTopology(atb1.build("JOB"), null);

        try {
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        ac.closeSession();

        // lce.shutdown();
    }
}
