package de.tuberlin.aura.demo.client;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterSimulator;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.task.common.DataConsumer;
import de.tuberlin.aura.core.task.common.DataProducer;
import de.tuberlin.aura.core.task.common.TaskDriverContext;
import de.tuberlin.aura.core.task.common.TaskInvokeable;
import de.tuberlin.aura.core.topology.AuraDirectedGraph;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;


public class TestClient {

    // Disallow instantiation.
    private TestClient() {
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
        public void open() throws Throwable {

        }


        @Override
        public void run() throws Throwable {

            final UUID taskID = driverContext.taskDescriptor.taskID;

            //int i = 0;
            //while (i++ < 1500 && isInvokeableRunning()) {
            //    final List<Descriptors.TaskDescriptor> outputs = driverContext.taskBindingDescriptor.outputGateBindings.get(0);
                /*for (int index = 0; index < outputs.size(); ++index) {
                    final UUID outputTaskID = getTaskID(0, index);
                    final MemoryManager.MemoryView buffer = producer.alloc();
                    final IOEvents.DataIOEvent outputBuffer = new IOEvents.TransferBufferEvent(taskID, outputTaskID, buffer);
                    producer.emit(0, index, outputBuffer);
                }*/
            //}
        }

        @Override
        public void close() throws Throwable {
            //producer.done();
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
        public void open() throws Throwable {
            consumer.openGate(0);
        }

        @Override
        public void run() throws Throwable {
            while (!consumer.isExhausted() && isInvokeableRunning()) {
                final IOEvents.TransferBufferEvent buffer = consumer.absorb(0);
                //LOG.info("received: " + buffer);
                if (buffer != null)
                    buffer.buffer.free();
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
        final LocalClusterSimulator lce =
                new LocalClusterSimulator(LocalClusterSimulator.ExecutionMode.EXECUTION_MODE_SINGLE_PROCESS, true, zookeeperAddress, 4);
        final AuraClient ac = new AuraClient(zookeeperAddress, 25340, 26340);

        final AuraDirectedGraph.AuraTopologyBuilder atb1 = ac.createTopologyBuilder();

        atb1.addNode(new AuraDirectedGraph.Node(UUID.randomUUID(), "Task1", 1, 1), Task1Exe.class);
        //.connectTo("Task2", AuraDirectedGraph.Edge.TransferType.ALL_TO_ALL);
        //.addNode(new AuraDirectedGraph.Node(UUID.randomUUID(), "Task2", 1, 1), Task2Exe.class);

        final AuraDirectedGraph.AuraTopology at1 = atb1.build("Job 1", EnumSet.of(AuraDirectedGraph.AuraTopology.MonitoringType.NO_MONITORING));

        ac.submitTopology(at1, null);

        try {
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        ac.closeSession();

        lce.shutdown();
    }

}
