package de.tuberlin.aura.taskmanager;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.task.common.TaskContext;
import de.tuberlin.aura.core.task.common.TaskInvokeable;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskState;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskTransition;
import de.tuberlin.aura.taskmanager.TaskEvents.TaskStateTransitionEvent;

public final class TaskExecutionUnit {

    //---------------------------------------------------
    // Inner Classes.
    //---------------------------------------------------

    private final class ExecutionUnitRunner implements Runnable {

        @Override
        public void run() {

            while( isExecutionUnitRunning.get() ) {

                try {
                    executingTaskContext = taskQueue.take();
                } catch (InterruptedException e) {
                    LOG.info( e );
                }

                // check precondition.
                if( executingTaskContext == null )
                    throw new IllegalStateException( "context == null" );
                if( executingTaskContext.state != TaskState.TASK_STATE_READY )
                    throw new IllegalStateException( "task is not in state ready" );

                // create instance of that task and execute it.
                TaskInvokeable invokeable = null;
                try {
                    invokeable = executingTaskContext.invokeableClass.getConstructor( TaskContext.class, Logger.class )
                        .newInstance( executingTaskContext, LOG );
                } catch( Exception e ) {
                    throw new IllegalStateException( e );
                }

                // check instance.
                if( invokeable == null )
                    throw new IllegalStateException( "invokeable == null" );

                executingTaskContext.dispatcher.dispatchEvent(
                        new TaskStateTransitionEvent( TaskTransition.TASK_TRANSITION_RUN ) );

                try {
                    invokeable.execute();
                } catch( Exception e ) {

                    LOG.error( e );

                    executingTaskContext.dispatcher.dispatchEvent(
                            new TaskStateTransitionEvent( TaskTransition.TASK_TRANSITION_FAILURE ) );

                    return;

                } finally {
                    invokeable = null; // let the instance be collected from gc.
                }

                executingTaskContext.dispatcher.dispatchEvent(
                        new TaskStateTransitionEvent( TaskTransition.TASK_TRANSITION_FINISH ) );
            }
        }
    }

    //---------------------------------------------------
    // Constructors.
    //---------------------------------------------------

    public TaskExecutionUnit( final int executionUnitID ) {
        // sanity check.
        if( executionUnitID < 0 )
            throw new IllegalArgumentException( "executionUnitID < 0" );

        this.executionUnitID = executionUnitID;

        this.executor = new Thread( new ExecutionUnitRunner() );

        this.taskQueue = new LinkedBlockingQueue<TaskContext>();

        this.isExecutionUnitRunning = new AtomicBoolean( false );

        this.executingTaskContext = null;
    }

    //---------------------------------------------------
    // Fields.
    //---------------------------------------------------

    private static final Logger LOG = Logger.getLogger( TaskExecutionUnit.class );

    private final int executionUnitID;

    private final Thread executor;

    private final BlockingQueue<TaskContext> taskQueue;

    private final AtomicBoolean isExecutionUnitRunning;

    private TaskContext executingTaskContext;

    //---------------------------------------------------
    // Public.
    //---------------------------------------------------

    public void start() {
        // check preconditions.
        if( executor.isAlive() )
            throw new IllegalStateException( "executor is already running" );

        isExecutionUnitRunning.set( true );

        executor.start();
    }

    public void enqueueTask( final TaskContext context ) {
        // sanity check.
        if( context == null )
            throw new IllegalArgumentException( "context == null" );

        taskQueue.add( context );
    }

    public void stop() {
        // check preconditions.
        if( !executor.isAlive() )
            throw new IllegalStateException( "executor is not running" );

        isExecutionUnitRunning.set( false );
    }

    public int getNumberOfEnqueuedTasks() {
        return taskQueue.size() + ( executingTaskContext != null ? 1 : 0 );
    }

    public int getExecutionUnitID() {
        return executionUnitID;
    }
}
