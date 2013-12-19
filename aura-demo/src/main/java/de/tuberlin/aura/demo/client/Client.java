package de.tuberlin.aura.demo.client;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

import de.tuberlin.aura.core.client.AuraClient;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.AuraTopologyBuilder;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.Edge;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.Node;
import de.tuberlin.aura.core.iosystem.IOEvents.DataBufferEvent;
import de.tuberlin.aura.core.task.common.TaskContext;
import de.tuberlin.aura.core.task.common.TaskInvokeable;
import de.tuberlin.aura.demo.deployment.LocalDeployment;


public final class Client {

    private static final Logger LOG = Logger.getRootLogger();

    // Disallow Instantiation.
    private Client() {}

    /**
     *
     */
    public static class Task1Exe extends TaskInvokeable {

        public Task1Exe( final TaskContext context, final Logger LOG ) {
            super( context, LOG );
        }

        @Override
        public void execute() throws Exception {
            for( int i = 0; i < 100; ++i ) {
                final byte[] data = new byte[65536];

                final DataBufferEvent dm = new DataBufferEvent( UUID.randomUUID(), context.task.uid,
                        context.taskBinding.outputGateBindings.get( 0 ).get( 0 ).uid, data );

                context.outputGates.get( 0 ).writeDataToChannel( 0, dm );

                try {
                    Thread.sleep( 100 );
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     *
     */
    public static class Task2Exe extends TaskInvokeable {

        public Task2Exe( final TaskContext context, final Logger LOG ) {
            super( context, LOG );
        }

        @Override
        public void execute() throws Exception {
            for( int i = 0; i < 100; ++i ) {
                final byte[] data = new byte[65536];
                final DataBufferEvent dm = new DataBufferEvent( UUID.randomUUID(), context.task.uid,
                        context.taskBinding.outputGateBindings.get( 0 ).get( 0 ).uid, data );
                context.outputGates.get( 0 ).writeDataToChannel( 0, dm );
                try {
                    Thread.sleep( 100 );
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     *
     */
    public static class Task3Exe extends TaskInvokeable {

        public Task3Exe( final TaskContext context, final Logger LOG ) {
            super( context, LOG );
        }

        @Override
        public void execute() throws Exception {
            for( int i = 0; i < 100; ++i ) {
                final BlockingQueue<DataBufferEvent> inputMsgs1 = context.inputGates.get( 0 ).getInputQueue();
                final BlockingQueue<DataBufferEvent> inputMsgs2 = context.inputGates.get( 1 ).getInputQueue();

                try {
                    final DataBufferEvent dm1 = inputMsgs1.take();
                    final DataBufferEvent dm2 = inputMsgs2.take();
                    LOG.info( "input1: received data message " + dm1.messageID + " from task " + dm1.srcTaskID );
                    LOG.info( "input2: received data message " + dm2.messageID + " from task " + dm2.srcTaskID );
                    final byte[] data = new byte[65536];
                    final DataBufferEvent dmOut = new DataBufferEvent( UUID.randomUUID(), context.task.uid,
                            context.taskBinding.outputGateBindings.get( 0 ).get( 0 ).uid, data );
                    context.outputGates.get( 0 ).writeDataToChannel( 0, dmOut );
                } catch (InterruptedException e) {
                    LOG.info( e );
                }
            }
        }
    }

    /**
     *
     */
    public static class Task4Exe extends TaskInvokeable {

        public Task4Exe( final TaskContext context, final Logger LOG ) {
            super( context, LOG );
        }

        @Override
        public void execute() throws Exception {
            for( int i = 0; i < 100; ++i ) {
                final BlockingQueue<DataBufferEvent> inputMsgs = context.inputGates.get( 0 ).getInputQueue();
                try {
                    final DataBufferEvent dm = inputMsgs.take();
                    LOG.info( "received data message " + dm.messageID + " from task " + dm.srcTaskID );
                } catch (InterruptedException e) {
                    LOG.info( e );
                }
            }
        }
    }

    //---------------------------------------------------
    // Main.
    //---------------------------------------------------

    public static void main( String[] args ) {

        final SimpleLayout layout = new SimpleLayout();
        final ConsoleAppender consoleAppender = new ConsoleAppender( layout );
        LOG.addAppender( consoleAppender );

        // Run the demo:
        // Start WM
        // Start TM 1-4
        // Start Client

        final AuraClient ac = new AuraClient( LocalDeployment.MACHINE_6_DESCRIPTOR, LocalDeployment.MACHINE_5_DESCRIPTOR );

        final AuraTopologyBuilder atb1 = ac.createTopologyBuilder();
        atb1.addNode( new Node( UUID.randomUUID(), "Task1", Task1Exe.class, 1, 1 ) )
            .connectTo( "Task3", Edge.TransferType.POINT_TO_POINT )
            .addNode( new Node( UUID.randomUUID(), "Task2", Task2Exe.class, 1, 1 ) )
            .connectTo( "Task3", Edge.TransferType.POINT_TO_POINT )
            .addNode( new Node( UUID.randomUUID(), "Task3", Task3Exe.class, 1, 1 ) )
            .connectTo( "Task4", Edge.TransferType.POINT_TO_POINT )
            .addNode( new Node( UUID.randomUUID(), "Task4", Task4Exe.class, 1, 1 ) );

        final AuraTopology at1 = atb1.build();
        ac.submitTopology( at1 );

        /*final AuraTopologyBuilder atb2 = ac.createTopologyBuilder();
        atb2.addNode( new Node( UUID.randomUUID(), "Task1", Task1Exe.class, 1, 1 ) )
            .connectTo( "Task4", Edge.TransferType.POINT_TO_POINT )
            .addNode( new Node( UUID.randomUUID(), "Task4", Task4Exe.class, 1, 1 ) );

        final AuraTopology at2 = atb2.build();
        ac.submitTopology( at2 );*/

        /* With Loops... (not working, loops not yet implemented in the runtime)
        final AuraTopologyBuilder atb = ac.createTopologyBuilder();
        atb.addNode( new Node( "Task1", Task1Exe.class, 1 ) )
           .connectTo( "Task3", Edge.TransferType.POINT_TO_POINT )
           .addNode( new Node( "Task2", Task2Exe.class, 1 ) )
           .connectTo( "Task3", Edge.TransferType.POINT_TO_POINT )
           .addNode( new Node( "Task3", Task3Exe.class, 1 ) )
           .connectTo( "Task4", Edge.TransferType.POINT_TO_POINT )
           .and().connectTo( "Task2", Edge.TransferType.POINT_TO_POINT, Edge.EdgeType.BACKWARD_EDGE ) // form a loop!!
           .addNode( new Node( "Task4", Task4Exe.class, 1 ) )
           .connectTo( "Task2", Edge.TransferType.POINT_TO_POINT, Edge.EdgeType.BACKWARD_EDGE ); // form a loop!!
        */
    }
}
