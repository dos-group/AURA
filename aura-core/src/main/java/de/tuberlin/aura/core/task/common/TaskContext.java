package de.tuberlin.aura.core.task.common;

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskBindingDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDescriptor;
import de.tuberlin.aura.core.iosystem.IOMessages.DataMessage;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskState;

/**
 * 
 */
public final class TaskContext {	
	
	public TaskContext( final TaskDescriptor task, 
						final TaskBindingDescriptor taskBinding,
						final IEventHandler handler, 
						final IEventDispatcher dispatcher, 
						final Class<? extends TaskInvokeable> invokeableClass ) {
		
		// sanity check.
		if( task == null )
			throw new IllegalArgumentException( "task == null" );	
		if( taskBinding == null )
			throw new IllegalArgumentException( "taskBinding == null" );
		if( handler == null )
			throw new IllegalArgumentException( "taskEventListener == null" );
		if( dispatcher == null )
			throw new IllegalArgumentException( "taskEventListener == null" );
		if( invokeableClass == null )
			throw new IllegalArgumentException( "invokeableClass == null" );
		
		this.task = task;
		
		this.taskBinding = taskBinding;
		
		this.handler = handler;
		
		this.dispatcher = dispatcher;
		
		this.state = TaskState.TASK_STATE_NOT_CONNECTED;
		
		this.invokeableClass = invokeableClass;
		
		if( taskBinding.inputGates.size() > 0 ) {
			
			inputChannels = new ArrayList<List<Channel>>( taskBinding.inputGates.size() );			
			
			inputQueues = new ArrayList<BlockingQueue<DataMessage>>( taskBinding.inputGates.size() );
			
			for( final List<TaskDescriptor> inputGate : taskBinding.inputGates ) {				
				
				final List<Channel> channelListPerGate = new ArrayList<Channel>( inputGate.size() );
				for( int i = 0; i < inputGate.size(); ++i ) {
					channelListPerGate.add( null );
				}
				
				inputChannels.add( channelListPerGate );
				
				for( int i = 0; i < inputGate.size(); ++i )
					inputQueues.add( new LinkedBlockingQueue<DataMessage>() );
			}

		} else {
			
			this.inputQueues = null;
			
			this.inputChannels = null;
		}
		
		if( taskBinding.outputGates.size() > 0 ) {
			
			outputChannels = new ArrayList<List<Channel>>( taskBinding.outputGates.size() );
			
			for( final List<TaskDescriptor> outputGate : taskBinding.outputGates ) {
				
				final List<Channel> channelListPerGate = new ArrayList<Channel>( outputGate.size() );
				for( int i = 0; i < outputGate.size(); ++i ) {
					channelListPerGate.add( null );
				}								
				
				outputChannels.add( channelListPerGate );
			}
			
		} else {
			
			outputChannels = null;
		}
	}
	
	public final TaskDescriptor task;
	
	public final TaskBindingDescriptor taskBinding;
	
	public final IEventHandler handler;
	
	public final IEventDispatcher dispatcher; 
	
	public final Class<? extends TaskInvokeable> invokeableClass;
	
	public final List<List<Channel>> inputChannels;
	
	public final List<List<Channel>> outputChannels;

	public final List<BlockingQueue<DataMessage>> inputQueues; 		
	
	public TaskState state;
	
	@Override
	public String toString() {
		return (new StringBuilder())
				.append( "TaskContext = {" )
				.append( " task = " + task + ", " )
				.append( " taskBinding = " + taskBinding + ", " )
				.append( " state = " + state.toString() + ", " )				
				.append( " }" ).toString();
	}
}