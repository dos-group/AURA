package de.tuberlin.aura.taskmanager;

import de.tuberlin.aura.taskmanager.Contexts.TaskContext;

public abstract class TaskInvokeable {
	
	public TaskInvokeable( final TaskContext context ) {
		// sanity check.
		if( context == null )
			throw new IllegalArgumentException( "context must not be null" );
		
		this.context = context;
	}
	
	protected final TaskContext context;

	public abstract void execute() throws Exception;
}