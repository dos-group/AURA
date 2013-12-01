package de.tuberlin.aura.demo.taskmanager;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

import de.tuberlin.aura.core.iosystem.IOMessages.DataMessage;
import de.tuberlin.aura.demo.deployment.LocalDeployment;
import de.tuberlin.aura.taskmanager.Contexts.TaskContext;
import de.tuberlin.aura.taskmanager.TaskInvokeable;
import de.tuberlin.aura.taskmanager.TaskManager;

public class Task3 {

	public static final Logger LOG = Logger.getRootLogger();
	
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
	
	
	public static void main(String[] args) {
		
		final SimpleLayout layout = new SimpleLayout();
		final ConsoleAppender consoleAppender = new ConsoleAppender( layout );
		LOG.addAppender( consoleAppender );
		
		final TaskManager taskManager = new TaskManager( LocalDeployment.MACHINE_3_DESCRIPTOR ); 
		taskManager.installTask( LocalDeployment.TASK_3_DESCRIPTOR, LocalDeployment.TASK_3_BINDING, Task3Exe.class );
		//taskManager.installTask( LocalDeployment.TASK_2_DESCRIPTOR, LocalDeployment.TASK_2_BINDING, Task2Exe.class );	
	
	}
}
