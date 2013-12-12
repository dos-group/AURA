package de.tuberlin.aura.core.task.common;

import org.apache.log4j.Logger;


public abstract class TaskInvokeable {

    public TaskInvokeable( final TaskContext context, final Logger LOG ) {
        // sanity check.
        if( context == null )
            throw new IllegalArgumentException( "context == null" );
        if( LOG == null )
            throw new IllegalArgumentException( "LOG == null" );

        this.context = context;

        this.LOG = LOG;
    }

    protected final TaskContext context;

    protected final Logger LOG;

    public abstract void execute() throws Exception;
}
