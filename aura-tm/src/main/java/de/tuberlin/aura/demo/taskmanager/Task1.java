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

public class Task1 {

	public static final Logger LOG = Logger.getRootLogger();
	
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

	public static void main(String[] args) {

		final SimpleLayout layout = new SimpleLayout();
		final ConsoleAppender consoleAppender = new ConsoleAppender( layout );
		LOG.addAppender( consoleAppender );
				
		final TaskManager taskManager = new TaskManager( LocalDeployment.MACHINE_1_DESCRIPTOR ); 
		taskManager.installTask( LocalDeployment.TASK_1_DESCRIPTOR, LocalDeployment.TASK_1_BINDING, Task1Exe.class );		
	}
}
