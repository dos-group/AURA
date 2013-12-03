package de.tuberlin.aura.core.task.common;



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