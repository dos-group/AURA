package de.tuberlin.aura.demo.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.UUID;

import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.config.IConfigFactory;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.SimpleLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.RowRecordReader;
import de.tuberlin.aura.core.record.RowRecordWriter;
import de.tuberlin.aura.core.task.spi.*;
import de.tuberlin.aura.core.topology.Topology;

public final class LazyUnrollingTest {

    /**
     * Logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(LazyUnrollingTest.class);

    // Disallow Instantiation.
    private LazyUnrollingTest() {}

    public static final class TestRecord {

        public TestRecord() {}

        public int a;

        public int b;

        public int c;

        public String d;
    }

    /**
     *
     */
    public static class TaskMap1 extends AbstractInvokeable {

        private final IRecordWriter recordWriter;

        public TaskMap1(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {

            super(taskDriver, producer, consumer, LOG);

            final Partitioner.IPartitioner partitioner = new Partitioner.HashPartitioner(new int[] {0});

            this.recordWriter = new RowRecordWriter(driver, TestRecord.class, 0, partitioner);
        }

        @Override
        public void run() throws Throwable {

            recordWriter.begin();

            for (int i = 0; i < 5; ++i) {
                final TestRecord tr = new TestRecord();
                tr.a = i;
                tr.b = 101;
                tr.c = 102;
                tr.d = "TaskMap1";
                recordWriter.writeObject(tr);
            }

            recordWriter.end();
        }

        @Override
        public void close() throws Throwable {
            producer.done();
        }
    }

    /**
     * public static class TaskLeftInput extends AbstractInvokeable {
     * 
     * private final IRecordWriter recordWriter;
     * 
     * private final IRecordReader recordReader;
     * 
     * public TaskLeftInput(final ITaskDriver taskDriver, final IDataProducer producer, final
     * IDataConsumer consumer, final Logger LOG) {
     * 
     * super(taskDriver, producer, consumer, LOG);
     * 
     * final RowRecordModel.Partitioner partitioner = new RowRecordModel.HashPartitioner(new int[]
     * {0});
     * 
     * this.recordWriter = new RowRecordWriter(driver, TestRecord.class, partitioner);
     * 
     * this.recordReader = new RowRecordReader(taskDriver, 0); }
     * 
     * @Override public void open() throws Throwable { consumer.openGate(0); }
     * @Override public void run() throws Throwable {
     * 
     *           while (!consumer.isExhausted() && isInvokeableRunning()) {
     * 
     *           final IOEvents.TransferBufferEvent inEvent = consumer.absorb(0);
     * 
     *           if (inEvent != null) {
     * 
     *           inEvent.buffer.free();
     * 
     *           final MemoryView outBuffer = producer.getAllocator().allocBlocking();
     * 
     *           producer.store(outBuffer); } } } }
     */


    /**
     *
     */
    public static class TaskRightInput extends AbstractInvokeable {

        private final IRecordWriter recordWriter;

        public TaskRightInput(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {

            super(taskDriver, producer, consumer, LOG);

            final Partitioner.IPartitioner partitioner = new Partitioner.HashPartitioner(new int[] {0});

            this.recordWriter = new RowRecordWriter(driver, TestRecord.class, 0, partitioner);
        }

        @Override
        public void run() throws Throwable {

            recordWriter.begin();

            for (int i = 0; i < 5; ++i) {
                final TestRecord tr = new TestRecord();
                tr.a = i;
                tr.b = 101;
                tr.c = 102;
                tr.d = "TaskRightInput";
                recordWriter.writeObject(tr);
            }

            recordWriter.end();
        }

        @Override
        public void close() throws Throwable {
            producer.done();
        }
    }

    /**
     *
     */
    public static class TaskBinaryInput extends AbstractInvokeable {

        private final IRecordWriter recordWriter;

        private final IRecordReader recordReaderLeft;

        private final IRecordReader recordReaderRight;

        public TaskBinaryInput(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {

            super(taskDriver, producer, consumer, LOG);

            final Partitioner.IPartitioner partitioner = new Partitioner.HashPartitioner(new int[] {0});

            this.recordWriter = new RowRecordWriter(driver, TestRecord.class, 0, partitioner);

            this.recordReaderLeft = new RowRecordReader(driver, 0);

            this.recordReaderRight = new RowRecordReader(driver, 1);
        }

        @Override
        public void open() throws Throwable {

            consumer.openGate(0);

            consumer.openGate(1);
        }

        @Override
        public void run() throws Throwable {

            recordWriter.begin();

            recordReaderRight.begin();

            recordReaderLeft.begin();

            while (!recordReaderLeft.finished() && !recordReaderRight.finished()) {

                final TestRecord leftObj = !recordReaderLeft.finished() ? (TestRecord) recordReaderLeft.readObject() : null;

                final TestRecord rightObj = !recordReaderRight.finished() ? (TestRecord) recordReaderRight.readObject() : null;

                final TestRecord tr = new TestRecord();
                tr.a = leftObj != null ? leftObj.a : 2;
                tr.b = 101;
                tr.c = 102;
                tr.d = (leftObj != null ? leftObj.d : "") + (rightObj != null ? rightObj.d : "");
                recordWriter.writeObject(tr);
            }

            recordReaderRight.end();

            recordReaderLeft.end();

            recordWriter.end();
        }

        @Override
        public void close() throws Throwable {
            producer.done();
        }
    }

    /**
     *
     */
    public static class TaskMap3 extends AbstractInvokeable {

        private IRecordReader recordReader;

        public TaskMap3(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {

            super(taskDriver, producer, consumer, LOG);

            this.recordReader = new RowRecordReader(driver, 0);
        }

        @Override
        public void open() throws Throwable {
            consumer.openGate(0);
        }

        @Override
        public void run() throws Throwable {

            recordReader.begin();

            while (!recordReader.finished()) {
                final TestRecord obj = (TestRecord) recordReader.readObject();
                if (obj != null) {
                    String value3 = obj.d;
                    int value0 = obj.a;
                    if (driver.getNodeDescriptor().taskIndex == 0) {
                        System.out.println(driver.getNodeDescriptor().taskID + " -- value3: " + value3 + "   value0:" + value0);
                    }
                }
            }

            recordReader.end();
        }
    }

    // ---------------------------------------------------
    // Main.
    // ---------------------------------------------------

    public static void main(String[] args) {

        final SimpleLayout layout = new SimpleLayout();
        final ConsoleAppender consoleAppender = new ConsoleAppender(layout);

        final LocalClusterSimulator lcs = new LocalClusterSimulator(IConfigFactory.load(IConfig.Type.SIMULATOR));
        final AuraClient ac = new AuraClient(IConfigFactory.load(IConfig.Type.CLIENT));
        final Topology.AuraTopologyBuilder atb1 = ac.createTopologyBuilder();

        //@formatter:off
        atb1.addNode(new Topology.ComputationNode(UUID.randomUUID(), "TaskMap1", 2, 1), TaskMap1.class)
            .connectTo("LeftStorage", Topology.Edge.TransferType.POINT_TO_POINT)
            .addStorageNode(new Topology.StorageNode(UUID.randomUUID(), "LeftStorage", 2, 1));
        //.addNode(new Topology.ComputationNode(UUID.randomUUID(), "TaskLeftInput", 2, 1), TaskLeftInput.class);
        //@formatter:on

        final Topology.AuraTopology at1 = atb1.build("JOB 1");

        ac.submitTopology(at1, null);

        try {
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        final Topology.AuraTopologyBuilder atb2 = ac.createTopologyBuilder();

        //@formatter:off
        atb2.addNode(new Topology.ComputationNode(UUID.randomUUID(), "TaskRightInput", 2, 1), TaskRightInput.class)
            .connectTo("TaskBinaryInput", Topology.Edge.TransferType.POINT_TO_POINT)
            .connectFrom("LeftStorage", "TaskBinaryInput", Topology.Edge.TransferType.POINT_TO_POINT)
            .addNode(new Topology.ComputationNode(UUID.randomUUID(), "TaskBinaryInput", 2, 1), TaskBinaryInput.class)
            .connectTo("TaskMap3", Topology.Edge.TransferType.POINT_TO_POINT)
            .addNode(new Topology.ComputationNode(UUID.randomUUID(), "TaskMap3", 2, 1), TaskMap3.class);
        //@formatter:on

        final Topology.AuraTopology at2 = atb2.build("JOB 2");

        ac.submitToTopology(at1.topologyID, at2);

        try {
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        ac.closeSession();

        // atb1.addNode(new Topology.Node(taskNodeID, "Task1", 4, 1), TaskMap1.class);
        // .connectTo("Task3", Topology.Edge.TransferType.ALL_TO_ALL)
        // .addNode(new Topology.Node(UUID.randomUUID(), "Task2", 4, 1), TaskLeftInput.class)
        // .connectTo("Task3", Topology.Edge.TransferType.ALL_TO_ALL)
        // .addNode(new Topology.Node(UUID.randomUUID(), "Task3", 4, 1), TaskBinaryInput.class)
        // .connectTo("Task4", Topology.Edge.TransferType.ALL_TO_ALL)
        // .addNode(new Topology.Node(UUID.randomUUID(), "Task4", 4, 1), TaskMap3.class);
        lcs.shutdown();
    }
}

// public final class LazyUnrollingTest {
//
//
// private static final Logger LOG = LoggerFactory.getLogger(LazyUnrollingTest.class);
//
// // Disallow Instantiation.
// private LazyUnrollingTest() {}
//
// public static final class TestRecord {
//
// public TestRecord() {
// }
//
// public int a;
//
// public int b;
//
// public int c;
//
// public String d;
// }
//
// public static class TaskMap1 extends AbstractInvokeable {
//
// private final IRecordWriter recordWriter;
//
// public TaskMap1(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer
// consumer, final Logger LOG) {
//
// super(taskDriver, producer, consumer, LOG);
//
// final RowRecordModel.Partitioner partitioner = new RowRecordModel.HashPartitioner(new int[] {0});
//
// this.recordWriter = new RowRecordWriter(driver, TestRecord.class, partitioner);
// }
//
// @Override
// public void run() throws Throwable {
//
// final UUID srcTaskID = driver.getNodeDescriptor().taskID;
//
// final List<Descriptors.AbstractNodeDescriptor> outputBinding =
// driver.getBindingDescriptor().outputGateBindings.get(0);
//
// int i = 0;
// while (i++ < 10 && isInvokeableRunning()) {
//
// for (int index = 0; index < outputBinding.size(); ++index) {
//
// final UUID dstTaskID = getTaskID(0, index);
//
// final MemoryView outBuffer = producer.getAllocator().allocBlocking();
//
// final IOEvents.TransferBufferEvent outEvent = new IOEvents.TransferBufferEvent(srcTaskID,
// dstTaskID, outBuffer);
//
// producer.emit(0, index, outEvent);
// }
// }
// }
//
// @Override
// public void close() throws Throwable {
// producer.done();
// }
// }
//
// public static class TaskLeftInput extends AbstractInvokeable {
//
// private final IRecordWriter recordWriter;
//
// private final IRecordReader recordReader;
//
// public TaskLeftInput(final ITaskDriver taskDriver, final IDataProducer producer, final
// IDataConsumer consumer, final Logger LOG) {
//
// super(taskDriver, producer, consumer, LOG);
//
// final RowRecordModel.Partitioner partitioner = new RowRecordModel.HashPartitioner(new int[] {0});
//
// this.recordWriter = new RowRecordWriter(driver, TestRecord.class, partitioner);
//
// this.recordReader = new RowRecordReader(taskDriver, 0);
// }
//
// @Override
// public void open() throws Throwable {
// consumer.openGate(0);
// }
//
// @Override
// public void run() throws Throwable {
//
// while (!consumer.isExhausted() && isInvokeableRunning()) {
//
// final IOEvents.TransferBufferEvent inEvent = consumer.absorb(0);
//
// if (inEvent != null) {
//
// inEvent.buffer.free();
//
// final MemoryView outBuffer = producer.getAllocator().allocBlocking();
//
// producer.store(outBuffer);
// }
// }
// }
// }
//
// public static class TaskRightInput extends AbstractInvokeable {
//
// private final IRecordWriter recordWriter;
//
// public TaskRightInput(final ITaskDriver taskDriver, final IDataProducer producer, final
// IDataConsumer consumer, final Logger LOG) {
//
// super(taskDriver, producer, consumer, LOG);
//
// final RowRecordModel.Partitioner partitioner = new RowRecordModel.HashPartitioner(new int[] {0});
//
// this.recordWriter = new RowRecordWriter(driver, TestRecord.class, partitioner);
// }
//
// @Override
// public void run() throws Throwable {
//
// final UUID srcTaskID = driver.getNodeDescriptor().taskID;
//
// final List<Descriptors.AbstractNodeDescriptor> outputBinding =
// driver.getBindingDescriptor().outputGateBindings.get(0);
//
// int i = 0;
// while (i++ < 10 && isInvokeableRunning()) {
//
// for (int index = 0; index < outputBinding.size(); ++index) {
//
// final UUID dstTaskID = getTaskID(0, index);
//
// final MemoryView outBuffer = producer.getAllocator().allocBlocking();
//
// final IOEvents.TransferBufferEvent outEvent = new IOEvents.TransferBufferEvent(srcTaskID,
// dstTaskID, outBuffer);
//
// producer.emit(0, index, outEvent);
// }
// }
// }
//
// @Override
// public void close() throws Throwable {
// producer.done();
// }
// }
//
// public static class TaskBinaryInput extends AbstractInvokeable {
//
// private final IRecordWriter recordWriter;
//
// private final IRecordReader recordReaderLeft;
//
// private final IRecordReader recordReaderRight;
//
// public TaskBinaryInput(final ITaskDriver taskDriver, final IDataProducer producer, final
// IDataConsumer consumer, final Logger LOG) {
//
// super(taskDriver, producer, consumer, LOG);
//
// final RowRecordModel.Partitioner partitioner = new RowRecordModel.HashPartitioner(new int[] {0});
//
// this.recordWriter = new RowRecordWriter(driver, TestRecord.class, partitioner);
//
// this.recordReaderLeft = new RowRecordReader(taskDriver, 0);
//
// this.recordReaderRight = new RowRecordReader(taskDriver, 1);
// }
//
// @Override
// public void open() throws Throwable {
// consumer.openGate(0);
// consumer.openGate(1);
// }
//
// @Override
// public void run() throws Throwable {
//
// final UUID srcTaskID = driver.getNodeDescriptor().taskID;
//
// final List<Descriptors.AbstractNodeDescriptor> outputBinding =
// driver.getBindingDescriptor().outputGateBindings.get(0);
//
// while (!consumer.isExhausted() && isInvokeableRunning()) {
//
// final IOEvents.TransferBufferEvent inBufferLeft = consumer.absorb(0);
//
// final IOEvents.TransferBufferEvent inBufferRight = consumer.absorb(1);
//
// if (inBufferLeft != null) {
//
// inBufferLeft.buffer.free();
// }
//
// if (inBufferRight != null) {
//
// inBufferRight.buffer.free();
// }
//
//
// if (inBufferLeft != null && inBufferRight != null) {
//
// for (int index = 0; index < outputBinding.size(); ++index) {
//
// final UUID dstTaskID = getTaskID(0, index);
//
// final MemoryView outBuffer = producer.getAllocator().allocBlocking();
//
// final IOEvents.TransferBufferEvent outEvent = new IOEvents.TransferBufferEvent(srcTaskID,
// dstTaskID, outBuffer);
//
// producer.emit(0, index, outEvent);
// }
// }
// }
// }
//
// @Override
// public void close() throws Throwable {
// producer.done();
// }
// }
//
// public static class TaskMap3 extends AbstractInvokeable {
//
// private IRecordReader recordReader;
//
// public TaskMap3(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer
// consumer, final Logger LOG) {
//
// super(taskDriver, producer, consumer, LOG);
//
// this.recordReader = new RowRecordReader(taskDriver, 0);
// }
//
// @Override
// public void open() throws Throwable {
// consumer.openGate(0);
// }
//
// @Override
// public void run() throws Throwable {
//
// while (!consumer.isExhausted() && isInvokeableRunning()) {
//
// final IOEvents.TransferBufferEvent inEvent = consumer.absorb(0);
//
// if (inEvent != null) {
//
// LOG.info("----------------> READ BUFFER");
//
// inEvent.buffer.free();
// }
// }
// }
// }
//
// // ---------------------------------------------------
// // Main.
// // ---------------------------------------------------
//
// public static void main(String[] args) {
//
// final SimpleLayout layout = new SimpleLayout();
// final ConsoleAppender consoleAppender = new ConsoleAppender(layout);
//
// // Local
// final String measurementPath = "/home/tobias/Desktop/logs";
// final LocalClusterSimulator lce = new
// LocalClusterSimulator(LocalClusterSimulator.ExecutionMode.local, true,
// zookeeperAddress, 4);
//
// final AuraClient ac = new AuraClient(zookeeperAddress, 10000, 11111);
//
// final Topology.AuraTopologyBuilder atb1 = ac.createTopologyBuilder();
//
// atb1.addNode(new Topology.ComputationNode(UUID.randomUUID(), "TaskMap1", 2, 1), TaskMap1.class).
// connectTo("LeftStorage", Topology.Edge.TransferType.POINT_TO_POINT)
// .addStorageNode(new Topology.StorageNode(UUID.randomUUID(), "LeftStorage", 2, 1));
// //.addNode(new Topology.ComputationNode(UUID.randomUUID(), "TaskLeftInput", 2, 1),
// TaskLeftInput.class);
//
// final Topology.AuraTopology at1 = atb1.build("JOB 1");
//
// ac.submitTopology(at1, null);
//
// try {
// new BufferedReader(new InputStreamReader(System.in)).readLine();
// } catch (IOException e) {
// e.printStackTrace();
// }
//
// final Topology.AuraTopologyBuilder atb2 = ac.createTopologyBuilder();
//
// atb2.addNode(new Topology.ComputationNode(UUID.randomUUID(), "TaskRightInput", 2, 1),
// TaskRightInput.class)
// .connectTo("TaskBinaryInput", Topology.Edge.TransferType.POINT_TO_POINT)
// .connectFrom("LeftStorage", "TaskBinaryInput", Topology.Edge.TransferType.POINT_TO_POINT)
// .addNode(new Topology.ComputationNode(UUID.randomUUID(), "TaskBinaryInput", 2, 1),
// TaskBinaryInput.class)
// .connectTo("TaskMap3", Topology.Edge.TransferType.POINT_TO_POINT)
// .addNode(new Topology.ComputationNode(UUID.randomUUID(), "TaskMap3", 2, 1), TaskMap3.class);
//
// final Topology.AuraTopology at2 = atb2.build("JOB 2");
//
// ac.submitToTopology(at1.topologyID, at2);
//
// try {
// new BufferedReader(new InputStreamReader(System.in)).readLine();
// } catch (IOException e) {
// e.printStackTrace();
// }
//
// ac.closeSession();
//
// //atb1.addNode(new Topology.Node(taskNodeID, "Task1", 4, 1), TaskMap1.class);
// //.connectTo("Task3", Topology.Edge.TransferType.ALL_TO_ALL)
// //.addNode(new Topology.Node(UUID.randomUUID(), "Task2", 4, 1), TaskLeftInput.class)
// //.connectTo("Task3", Topology.Edge.TransferType.ALL_TO_ALL)
// //.addNode(new Topology.Node(UUID.randomUUID(), "Task3", 4, 1), TaskBinaryInput.class)
// //.connectTo("Task4", Topology.Edge.TransferType.ALL_TO_ALL)
// //.addNode(new Topology.Node(UUID.randomUUID(), "Task4", 4, 1), TaskMap3.class);
// // lce.shutdown();
// }
// }
