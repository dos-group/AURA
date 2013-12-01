package de.tuberlin.aura.taskmanager;

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskBindingDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDescriptor;
import de.tuberlin.aura.core.iosystem.IOMessages.DataMessage;
import de.tuberlin.aura.taskmanager.TaskStateMachine.TaskState;

public final class Contexts {

	// Disallow instantiation.
	private Contexts() {}

	/**
	 * 
	 */
	public static final class TaskContext {	
		
		public TaskContext( final TaskDescriptor task, final TaskBindingDescriptor taskBinding,
				final IEventHandler handler, final IEventDispatcher dispatcher, 
				final Class<? extends TaskInvokeable> invokeableClass ) {
			// sanity check.
			if( task == null )
				throw new IllegalArgumentException( "task must not be null" );	
			if( taskBinding == null )
				throw new IllegalArgumentException( "taskBinding must not be null" );
			if( handler == null )
				throw new IllegalArgumentException( "taskEventListener must not be null" );
			if( dispatcher == null )
				throw new IllegalArgumentException( "taskEventListener must not be null" );
			if( invokeableClass == null )
				throw new IllegalArgumentException( "invokeableClass must not be null" );
			
			this.task = task;
			
			this.taskBinding = taskBinding;
			
			this.handler = handler;
			
			this.dispatcher = dispatcher;
			
			this.state = TaskState.TASK_STATE_NOT_CONNECTED;
			
			this.invokeableClass = invokeableClass;
			
			if( taskBinding.inputs.size() > 0 ) {
				
				this.inputChannel = new Channel[taskBinding.inputs.size()];
				
				final List<BlockingQueue<DataMessage>> tmpInputQueues = 
						new ArrayList<BlockingQueue<DataMessage>>( taskBinding.inputs.size() );						
				// TODO: Give the queues a fixed size!
				for( int i = 0; i < taskBinding.inputs.size(); ++i )
					tmpInputQueues.add( new LinkedBlockingQueue<DataMessage>() ); 								
				this.inputQueues = Collections.unmodifiableList( tmpInputQueues );
				
			} else {
				this.inputQueues = null;
				this.inputChannel = null;
			}
				
			if( taskBinding.outputs.size() > 0 ) {
				this.outputChannel = new Channel[taskBinding.outputs.size()];
			} else {
				this.outputChannel = null;
			}
		}
		
		public final TaskDescriptor task;
		
		public final TaskBindingDescriptor taskBinding;
		
		public final IEventHandler handler;
		
		public final IEventDispatcher dispatcher; 
		
		public final Class<? extends TaskInvokeable> invokeableClass;
		
		public final Channel[] inputChannel;
		
		public final Channel[] outputChannel;

		public final List<BlockingQueue<DataMessage>> inputQueues; 		
		
		public TaskState state;
		
		@Override
		public String toString() {
			return (new StringBuilder())
					.append( "TaskContext = {" )
					.append( " task = " + task + ", " )
					.append( " taskBinding = " + taskBinding + ", " )
					.append( " }" ).toString();
		}
	}
}