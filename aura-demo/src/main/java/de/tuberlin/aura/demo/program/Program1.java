package de.tuberlin.aura.demo.program;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

import de.tuberlin.aura.core.client.AuraClient;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskBindingDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDescriptor;
import de.tuberlin.aura.core.iosystem.IOMessages.DataMessage;
import de.tuberlin.aura.core.task.common.TaskContext;
import de.tuberlin.aura.core.task.common.TaskInvokeable;
import de.tuberlin.aura.demo.deployment.LocalDeployment;

public final class Program1 {

	private static final Logger LOG = Logger.getRootLogger();
	
	// Disallow Instantiation.
	private Program1() {}
	
	/**
	 * 
	 */
	public static class Task1Exe extends TaskInvokeable {

		public Task1Exe( TaskContext context ) {
			super( context );
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

		public Task2Exe( TaskContext context ) {
			super( context );
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

		public Task3Exe( TaskContext context ) {
			super( context );
		}

		@Override
		public void execute() throws Exception {

			for( int i = 0; i < 100; ++i ) {
			
				final BlockingQueue<DataMessage> inputMsgs1 = context.inputQueues.get( 0 );			
				final BlockingQueue<DataMessage> inputMsgs2 = context.inputQueues.get( 1 );
				
				try {
				
					final DataMessage dm1 = inputMsgs1.take();
					final DataMessage dm2 = inputMsgs2.take();
	
					context.LOG.info( "input1: received data message " + dm1.messageID + " from task " + dm1.srcTaskID );
					context.LOG.info( "input2: received data message " + dm2.messageID + " from task " + dm2.srcTaskID );					
					
					final byte[] data = new byte[1024];
					final DataMessage dmOut = new DataMessage( UUID.randomUUID(), context.task.uid, 
							context.taskBinding.outputs.get( 0 ).uid, data );
				
					context.outputChannel[0].writeAndFlush( dmOut );
					
				} catch (InterruptedException e) {
					context.LOG.info( e );
				}		
			}
		}
	}
	
	/**
	 * 
	 */
	public static class Task4Exe extends TaskInvokeable {

		public Task4Exe( TaskContext context ) {
			super( context );
		}

		@Override
		public void execute() throws Exception {
		
			for( int i = 0; i < 100; ++i ) {			
				final BlockingQueue<DataMessage> inputMsgs = context.inputQueues.get( 0 );			
				try {			
					final DataMessage dm = inputMsgs.take();
					context.LOG.info( "received data message " + dm.messageID + " from task " + dm.srcTaskID );
				} catch (InterruptedException e) {
					context.LOG.info( e );
				}
			}
		}
	}
	
	/**
	 * 
	 */
	/*public static class Task5Exe extends TaskInvokeable {

		public Task5Exe( TaskContext context ) {
			super( context );
		}

		@Override
		public void execute() throws Exception {		
			for( int i = 0; i < 100; ++i ) {				
				final byte[] data = new byte[1024];			
				final DataMessage dm = new DataMessage( UUID.randomUUID(), context.task.uid, 
						context.taskBinding.outputs.get( 0 ).uid, data );			
				context.outputChannel[0].writeAndFlush( dm );				
				try {
					Thread.sleep( 1000 );
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}*/
	
	/**
	 * 
	 */
	/*public static class Task6Exe extends TaskInvokeable {

		public Task6Exe( TaskContext context ) {
			super( context );
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
	}*/
	
	//---------------------------------------------------
    // Main.
    //---------------------------------------------------	

	public static void main( String[] args ) {
		
        final SimpleLayout layout = new SimpleLayout();
        final ConsoleAppender consoleAppender = new ConsoleAppender( layout );
        LOG.addAppender( consoleAppender );

        final AuraClient client = new AuraClient( LocalDeployment.MACHINE_6_DESCRIPTOR, LocalDeployment.MACHINE_5_DESCRIPTOR );        
        
        final Class<?>[] userClasses = { 
        		Task1Exe.class, 							 
        		Task2Exe.class,         									 
        		Task3Exe.class,         									 
        		Task4Exe.class         									 
        		//Task5Exe.class 
        	};
        
        final TaskDescriptor[] taskDescriptors = { 
        		LocalDeployment.TASK_1_DESCRIPTOR,
        		LocalDeployment.TASK_2_DESCRIPTOR,
        		LocalDeployment.TASK_3_DESCRIPTOR,
        		LocalDeployment.TASK_4_DESCRIPTOR
        		//LocalDeployment.TASK_5_DESCRIPTOR 
        	};
        
        final TaskBindingDescriptor[] taskBindingDescriptors = { 
        		LocalDeployment.TASK_1_BINDING,
				LocalDeployment.TASK_2_BINDING,
				LocalDeployment.TASK_3_BINDING,
				LocalDeployment.TASK_4_BINDING
				//LocalDeployment.TASK_5_BINDING 
			};
        
        client.submitProgram( 
        		Arrays.asList( userClasses ), 
        		Arrays.asList( taskDescriptors ), 
        		Arrays.asList( taskBindingDescriptors ) 
        	);        
	}
}