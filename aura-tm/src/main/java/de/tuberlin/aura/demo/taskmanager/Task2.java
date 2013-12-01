package de.tuberlin.aura.demo.taskmanager;

import java.util.UUID;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

import de.tuberlin.aura.core.iosystem.IOMessages.DataMessage;
import de.tuberlin.aura.demo.deployment.LocalDeployment;
import de.tuberlin.aura.taskmanager.Contexts.TaskContext;
import de.tuberlin.aura.taskmanager.TaskInvokeable;
import de.tuberlin.aura.taskmanager.TaskManager;

public class Task2 {

	public static final Logger LOG = Logger.getRootLogger();
	
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
	
	public static class Task5Exe extends TaskInvokeable {

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
	}
	
	public static void main(String[] args) {
		
		final SimpleLayout layout = new SimpleLayout();
		final ConsoleAppender consoleAppender = new ConsoleAppender( layout );
		LOG.addAppender( consoleAppender );
		
		final TaskManager taskManager = new TaskManager( LocalDeployment.MACHINE_2_DESCRIPTOR ); 
		taskManager.installTask( LocalDeployment.TASK_2_DESCRIPTOR, LocalDeployment.TASK_2_BINDING, Task2Exe.class );
		taskManager.installTask( LocalDeployment.TASK_5_DESCRIPTOR, LocalDeployment.TASK_5_BINDING, Task5Exe.class );

	}
}
