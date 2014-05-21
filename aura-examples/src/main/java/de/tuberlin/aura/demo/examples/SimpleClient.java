package de.tuberlin.aura.demo.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.UUID;

import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.memory.test.BufferAllocator;
import de.tuberlin.aura.core.task.spi.*;
import de.tuberlin.aura.taskmanager.TaskRecordReader;
import de.tuberlin.aura.taskmanager.TaskRecordWriter;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.SimpleLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.client.api.AuraClient;
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
    public static class TaskMap1 extends AbstractTaskInvokeable {

        private final IRecordWriter recordWriter;

        public TaskMap1(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {

            super(taskDriver, producer, consumer, LOG);

            this.recordWriter = new TaskRecordWriter(BufferAllocator._64K);
        }

        @Override
        public void run() throws Throwable {

            final UUID srcTaskID = taskDriver.getTaskDescriptor().taskID;

            final List<Descriptors.TaskDescriptor> outputBinding = taskDriver.getTaskBindingDescriptor().outputGateBindings.get(0);

            int i = 0;
            while (i++ < 10 && isInvokeableRunning()) {

                for (int index = 0; index < outputBinding.size(); ++index) {

                    final UUID dstTaskID = getTaskID(0, index);

                    final MemoryView outBuffer = producer.alloc();

                    recordWriter.selectBuffer(outBuffer);

                    recordWriter.writeRecord(new Integer(100));

                    final IOEvents.TransferBufferEvent outEvent = new IOEvents.TransferBufferEvent(srcTaskID, dstTaskID, outBuffer);

                    producer.emit(0, index, outEvent);
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
    public static class TaskLeftInput extends AbstractTaskInvokeable {

        private final IRecordWriter recordWriter;

        private final IRecordReader recordReader;

        public TaskLeftInput(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {

            super(taskDriver, producer, consumer, LOG);

            this.recordWriter = new TaskRecordWriter(BufferAllocator._64K);

            this.recordReader = new TaskRecordReader(BufferAllocator._64K);
        }

        @Override
        public void open() throws Throwable {
            consumer.openGate(0);
        }

        @Override
        public void run() throws Throwable {

            while (!consumer.isExhausted() && isInvokeableRunning()) {

                final IOEvents.TransferBufferEvent inEvent = consumer.absorb(0);

                if (inEvent != null) {

                    recordReader.selectBuffer(inEvent.buffer);

                    final Integer value = recordReader.readRecord(Integer.class);

                    inEvent.buffer.free();

                    final MemoryView outBuffer = producer.alloc();

                    recordWriter.selectBuffer(outBuffer);

                    recordWriter.writeRecord(new Integer(value + 100));

                    producer.store(outBuffer);
                }
            }
        }
    }

    /**
     *
     */
    public static class TaskRightInput extends AbstractTaskInvokeable {

        private final IRecordWriter recordWriter;

        public TaskRightInput(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {

            super(taskDriver, producer, consumer, LOG);

            this.recordWriter = new TaskRecordWriter(BufferAllocator._64K);
        }

        @Override
        public void run() throws Throwable {

            final UUID srcTaskID = taskDriver.getTaskDescriptor().taskID;

            final List<Descriptors.TaskDescriptor> outputBinding = taskDriver.getTaskBindingDescriptor().outputGateBindings.get(0);

            int i = 0;
            while (i++ < 10 && isInvokeableRunning()) {

                for (int index = 0; index < outputBinding.size(); ++index) {

                    final UUID dstTaskID = getTaskID(0, index);

                    final MemoryView outBuffer = producer.alloc();

                    recordWriter.selectBuffer(outBuffer);

                    recordWriter.writeRecord(new Integer(50));

                    final IOEvents.TransferBufferEvent outEvent = new IOEvents.TransferBufferEvent(srcTaskID, dstTaskID, outBuffer);

                    producer.emit(0, index, outEvent);
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
    public static class TaskBinaryInput extends AbstractTaskInvokeable {

        private final IRecordWriter recordWriter;

        private final IRecordReader recordReaderLeft;

        private final IRecordReader recordReaderRight;

        public TaskBinaryInput(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {

            super(taskDriver, producer, consumer, LOG);

            this.recordWriter = new TaskRecordWriter(BufferAllocator._64K);

            this.recordReaderLeft = new TaskRecordReader(BufferAllocator._64K);

            this.recordReaderRight = new TaskRecordReader(BufferAllocator._64K);
        }

        @Override
        public void open() throws Throwable {
            consumer.openGate(0);
            consumer.openGate(1);
        }

        @Override
        public void run() throws Throwable {

            final UUID srcTaskID = taskDriver.getTaskDescriptor().taskID;

            final List<Descriptors.TaskDescriptor> outputBinding = taskDriver.getTaskBindingDescriptor().outputGateBindings.get(0);

            while (!consumer.isExhausted() && isInvokeableRunning()) {

                final IOEvents.TransferBufferEvent inBufferLeft = consumer.absorb(0);

                final IOEvents.TransferBufferEvent inBufferRight = consumer.absorb(1);


                Integer valueLeft = 0;

                if (inBufferLeft != null) {

                    recordReaderLeft.selectBuffer(inBufferLeft.buffer);

                    valueLeft = recordReaderLeft.readRecord(Integer.class);

                    inBufferLeft.buffer.free();
                }

                Integer valueRight = 0;

                if (inBufferRight != null) {

                    recordReaderRight.selectBuffer(inBufferRight.buffer);

                    valueRight = recordReaderRight.readRecord(Integer.class);

                    inBufferRight.buffer.free();
                }


                if (inBufferLeft != null && inBufferRight != null) {

                    for (int index = 0; index < outputBinding.size(); ++index) {

                        final UUID dstTaskID = getTaskID(0, index);

                        final MemoryView outBuffer = producer.alloc();

                        recordWriter.selectBuffer(outBuffer);

                        recordWriter.writeRecord(new Integer(valueLeft + valueRight));

                        final IOEvents.TransferBufferEvent outEvent = new IOEvents.TransferBufferEvent(srcTaskID, dstTaskID, outBuffer);

                        producer.emit(0, index, outEvent);
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
    public static class TaskMap3 extends AbstractTaskInvokeable {

        private IRecordReader recordReader;

        public TaskMap3(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {

            super(taskDriver, producer, consumer, LOG);

            this.recordReader = new TaskRecordReader(BufferAllocator._64K);
        }

        @Override
        public void open() throws Throwable {
            consumer.openGate(0);
        }

        @Override
        public void run() throws Throwable {

            while (!consumer.isExhausted() && isInvokeableRunning()) {

                final IOEvents.TransferBufferEvent inEvent = consumer.absorb(0);

                if (inEvent != null) {

                    recordReader.selectBuffer(inEvent.buffer);

                    final Integer value = recordReader.readRecord(Integer.class);

                    LOG.info("----------------> READ BUFFER: " + value);

                    inEvent.buffer.free();
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

        atb1.addNode(new AuraDirectedGraph.Node(UUID.randomUUID(), "TaskMap1", 2, 1), TaskMap1.class).
                connectTo("TaskLeftInput", AuraDirectedGraph.Edge.TransferType.POINT_TO_POINT)
            .addNode(new AuraDirectedGraph.Node(UUID.randomUUID(), "TaskLeftInput", 2, 1), TaskLeftInput.class);

        final AuraDirectedGraph.AuraTopology at1 = atb1.build("JOB 1");

        ac.submitTopology(at1, null);


        try {
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        final AuraDirectedGraph.AuraTopologyBuilder atb2 = ac.createTopologyBuilder();

        atb2.addNode(new AuraDirectedGraph.Node(UUID.randomUUID(), "TaskRightInput", 2, 1), TaskRightInput.class)
                .connectTo("TaskBinaryInput", AuraDirectedGraph.Edge.TransferType.ALL_TO_ALL)
            .connectFrom("TaskLeftInput", "TaskBinaryInput", AuraDirectedGraph.Edge.TransferType.ALL_TO_ALL)
            .addNode(new AuraDirectedGraph.Node(UUID.randomUUID(), "TaskBinaryInput", 2, 1), TaskBinaryInput.class)
                .connectTo("TaskMap3", AuraDirectedGraph.Edge.TransferType.ALL_TO_ALL)
            .addNode(new AuraDirectedGraph.Node(UUID.randomUUID(), "TaskMap3", 2, 1), TaskMap3.class);

        final AuraDirectedGraph.AuraTopology at2 = atb2.build("JOB 2");

        ac.submitToTopology(at1.topologyID, at2);

        try {
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        ac.closeSession();

        //atb1.addNode(new AuraDirectedGraph.Node(taskNodeID, "Task1", 4, 1), TaskMap1.class);
        //.connectTo("Task3", AuraDirectedGraph.Edge.TransferType.ALL_TO_ALL)
        //.addNode(new AuraDirectedGraph.Node(UUID.randomUUID(), "Task2", 4, 1), TaskLeftInput.class)
        //.connectTo("Task3", AuraDirectedGraph.Edge.TransferType.ALL_TO_ALL)
        //.addNode(new AuraDirectedGraph.Node(UUID.randomUUID(), "Task3", 4, 1), TaskBinaryInput.class)
        //.connectTo("Task4", AuraDirectedGraph.Edge.TransferType.ALL_TO_ALL)
        //.addNode(new AuraDirectedGraph.Node(UUID.randomUUID(), "Task4", 4, 1), TaskMap3.class);


        // lce.shutdown();
    }
}
