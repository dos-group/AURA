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
import de.tuberlin.aura.core.iosystem.IOMessages.DataMessage;
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
				final byte[] data = new byte[1024];			
				final DataMessage dm = new DataMessage( UUID.randomUUID(), context.task.uid, 
						context.taskBinding.outputs.get( 0 ).uid, data );
				context.outputChannel[0].writeAndFlush( dm );
				try {
					Thread.sleep( 500 );
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
				final byte[] data = new byte[1024];			
				final DataMessage dm = new DataMessage( UUID.randomUUID(), context.task.uid, 
						context.taskBinding.outputs.get( 0 ).uid, data );
				context.outputChannel[0].writeAndFlush( dm );
				try {
					Thread.sleep( 500 );
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
				final BlockingQueue<DataMessage> inputMsgs1 = context.inputQueues.get( 0 );			
				final BlockingQueue<DataMessage> inputMsgs2 = context.inputQueues.get( 1 );
				try {
					final DataMessage dm1 = inputMsgs1.take();
					final DataMessage dm2 = inputMsgs2.take();
					LOG.info( "input1: received data message " + dm1.messageID + " from task " + dm1.srcTaskID );
					LOG.info( "input2: received data message " + dm2.messageID + " from task " + dm2.srcTaskID );					
					final byte[] data = new byte[1024];
					final DataMessage dmOut = new DataMessage( UUID.randomUUID(), context.task.uid, 
							context.taskBinding.outputs.get( 0 ).uid, data );
					context.outputChannel[0].writeAndFlush( dmOut );
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
				final BlockingQueue<DataMessage> inputMsgs = context.inputQueues.get( 0 );			
				try {			
					final DataMessage dm = inputMsgs.take();
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
        // Start TM 1-4
        // Start WM
        // Start Client
        
        final AuraClient ac = new AuraClient( LocalDeployment.MACHINE_6_DESCRIPTOR, LocalDeployment.MACHINE_5_DESCRIPTOR ); 
        
        final AuraTopologyBuilder atb = ac.createTopologyBuilder();
        atb.addNode( new Node( "Task1", Task1Exe.class, 1 ) )
           .connectTo( "Task3", Edge.TransferType.POINT_TO_POINT )
           .addNode( new Node( "Task2", Task2Exe.class, 1 ) )
           .connectTo( "Task3", Edge.TransferType.POINT_TO_POINT )
           .addNode( new Node( "Task3", Task3Exe.class, 1 ) )
           .connectTo( "Task4", Edge.TransferType.POINT_TO_POINT )
           .addNode( new Node( "Task4", Task4Exe.class, 1 ) ); 
        
        final AuraTopology at = atb.build();
        ac.submitTopology( at );
         
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