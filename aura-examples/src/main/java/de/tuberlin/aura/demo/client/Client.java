package de.tuberlin.aura.demo.client;

import de.tuberlin.aura.client.api.AuraClient;
import de.tuberlin.aura.client.executors.LocalClusterExecutor;
import de.tuberlin.aura.client.executors.LocalClusterExecutor.LocalExecutionMode;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.AuraTopologyBuilder;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.Edge;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.Node;
import de.tuberlin.aura.core.iosystem.IOEvents.DataBufferEvent;
import de.tuberlin.aura.core.iosystem.IOEvents.DataEventType;
import de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent;
import de.tuberlin.aura.core.task.common.TaskContext;
import de.tuberlin.aura.core.task.common.TaskInvokeable;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.SimpleLayout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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

        public Task1Exe(final TaskContext context, final Logger LOG) {
            super(context, LOG);
        }

        @Override
        public void execute() throws Exception {

            final UUID taskID = getTaskID();
            final UUID outputTaskID = getOutputTaskID(0, 0);

            for (int i = 0; i < 10; ++i) {
                //final DataIOEvent outputBuffer = new DataBufferEvent(taskID, outputTaskID, new byte[65536]);
                final DataIOEvent outputBuffer = new DataBufferEvent(taskID, outputTaskID, new byte[100]);
                emit(0, 0, outputBuffer);

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    LOG.error(e);
                }
            }

            final DataIOEvent exhaustedEvent = new DataIOEvent(DataEventType.DATA_EVENT_SOURCE_EXHAUSTED, taskID, outputTaskID);
            emit(0, 0, exhaustedEvent);
        }
    }

    /**
     *
     */
    public static class Task2Exe extends TaskInvokeable {

        public Task2Exe(final TaskContext context, final Logger LOG) {
            super(context, LOG);
        }

        @Override
        public void execute() throws Exception {

            final UUID taskID = getTaskID();
            final UUID outputTaskID = getOutputTaskID(0, 0);

            for (int i = 0; i < 10; ++i) {
                final DataIOEvent outputBuffer = new DataBufferEvent(taskID, outputTaskID, new byte[100]);
                emit(0, 0, outputBuffer);

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    LOG.error(e);
                }
            }

            final DataIOEvent exhaustedEvent = new DataIOEvent(DataEventType.DATA_EVENT_SOURCE_EXHAUSTED, taskID, outputTaskID);
            emit(0, 0, exhaustedEvent);
        }
    }

    /**
     *
     */
    public static class Task3Exe extends TaskInvokeable {

        public Task3Exe(final TaskContext context, final Logger LOG) {
            super(context, LOG);
        }

        @Override
        public void execute() throws Exception {

            final UUID taskID = getTaskID();
            final UUID outputTaskID = getOutputTaskID(0, 0);

            //openGate(0);
            //openGate(1);

            boolean inputLeftActive = true, inputRightActive = true;

            DataIOEvent leftInputBuffer = null;
            DataIOEvent rightInputBuffer = null;

            while (inputLeftActive || inputRightActive) {
                leftInputBuffer = absorb(0);
                rightInputBuffer = absorb(1);

                LOG.info("input1: received data message from task " + leftInputBuffer.srcTaskID + "    > " + leftInputBuffer.type);
                LOG.info("input2: received data message from task " + rightInputBuffer.srcTaskID + "    > " + rightInputBuffer.type);

                inputLeftActive = !DataEventType.DATA_EVENT_SOURCE_EXHAUSTED.equals(leftInputBuffer.type);
                inputRightActive = !DataEventType.DATA_EVENT_SOURCE_EXHAUSTED.equals(rightInputBuffer.type);

                if (inputLeftActive || inputRightActive) {
                    final DataIOEvent outputBuffer = new DataBufferEvent(taskID, outputTaskID, new byte[100]);
                    emit(0, 0, outputBuffer);
                }

                checkIfSuspended();
            }

            final DataIOEvent exhaustedEvent = new DataIOEvent(DataEventType.DATA_EVENT_SOURCE_EXHAUSTED, taskID, outputTaskID);
            emit(0, 0, exhaustedEvent);

            //closeGate(0);
            //closeGate(1);
        }
    }

    /**
     *
     */
    public static class Task4Exe extends TaskInvokeable {

        public Task4Exe(final TaskContext context, final Logger LOG) {
            super(context, LOG);
        }

        @Override
        public void execute() throws Exception {

            //openGate(0);

            boolean inputActive = true;

            while (inputActive) {

                final DataIOEvent inputBuffer = absorb(0);

                LOG.info("received data message from task " + inputBuffer.srcTaskID);

                inputActive = !DataEventType.DATA_EVENT_SOURCE_EXHAUSTED.equals(inputBuffer.type);

                checkIfSuspended();
            }

            //closeGate(0);
        }
    }

    public static void SLEEP(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            LOG.error(e);
        }
    }

    // ---------------------------------------------------
    // Main.
    // ---------------------------------------------------

    public static void main(String[] args) {

        final SimpleLayout layout = new SimpleLayout();

        final PatternLayout pL = new PatternLayout("%-4r [%t] %-5p %c %x - %m%n");
        final ConsoleAppender consoleAppender = new ConsoleAppender(layout);
        LOG.addAppender(consoleAppender);

        final String zookeeperAddress = "localhost:2181";
        final LocalClusterExecutor lce = new LocalClusterExecutor(LocalExecutionMode.EXECUTION_MODE_SINGLE_PROCESS,
                true, zookeeperAddress, 4);
        final AuraClient ac = new AuraClient(zookeeperAddress, 25340, 26340);

        final AuraTopologyBuilder atb1 = ac.createTopologyBuilder();
        atb1.addNode(new Node(UUID.randomUUID(), "Task1", 1, 1), Task1Exe.class)
                .connectTo("Task3", Edge.TransferType.POINT_TO_POINT)
                .addNode(new Node(UUID.randomUUID(), "Task2", 1, 1), Task2Exe.class)
                .connectTo("Task3", Edge.TransferType.POINT_TO_POINT)
                .addNode(new Node(UUID.randomUUID(), "Task3", 1, 1), Task3Exe.class)
                .connectTo("Task4", Edge.TransferType.POINT_TO_POINT)
                .addNode(new Node(UUID.randomUUID(), "Task4", 1, 1), Task4Exe.class);

        final AuraTopology at1 = atb1.build("Job 1");
        ac.submitTopology(at1);

//        final AuraTopologyBuilder atb2 = ac.createTopologyBuilder();
//        atb2.addNode(new Node(UUID.randomUUID(), "Task1", 1, 1), Task1Exe.class)
//                .connectTo("Task4", Edge.TransferType.POINT_TO_POINT)
//                .addNode(new Node(UUID.randomUUID(), "Task4", 1, 1), Task4Exe.class);
//
//        final AuraTopology at2 = atb2.build("Job 2");
//        ac.submitTopology(at2);

        try {
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        lce.shutdown();
        /*
		 * With Loops... (not working, loops not yet implemented in the runtime)
		 * final AuraTopologyBuilder atb = ac.createTopologyBuilder();
		 * atb.addNode( new Node( "Task1", Task1Exe.class, 1 ) )
		 * .connectTo( "Task3", Edge.TransferType.POINT_TO_POINT )
		 * .addNode( new Node( "Task2", Task2Exe.class, 1 ) )
		 * .connectTo( "Task3", Edge.TransferType.POINT_TO_POINT )
		 * .addNode( new Node( "Task3", Task3Exe.class, 1 ) )
		 * .connectTo( "Task4", Edge.TransferType.POINT_TO_POINT )
		 * .and().connectTo( "Task2", Edge.TransferType.POINT_TO_POINT, Edge.EdgeType.BACKWARD_EDGE ) // form a loop!!
		 * .addNode( new Node( "Task4", Task4Exe.class, 1 ) )
		 * .connectTo( "Task2", Edge.TransferType.POINT_TO_POINT, Edge.EdgeType.BACKWARD_EDGE ); // form a loop!!
		 */
    }
}
