package de.tuberlin.aura.taskmanager;

import de.tuberlin.aura.core.iosystem.IOEvents.TaskStateTransitionEvent;
import de.tuberlin.aura.core.task.common.TaskInvokeable;
import de.tuberlin.aura.core.task.common.TaskRuntimeContext;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskState;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskTransition;

import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public final class TaskExecutionUnit {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private final class ExecutionUnitRunner implements Runnable {

        @Override
        public void run() {

            while (isExecutionUnitRunning.get()) {

                try {
                    context = taskQueue.take();
                } catch (InterruptedException e) {
                    LOG.info(e);
                }

                // check precondition.
                if (context == null) {
                    throw new IllegalStateException("context == null");
                }
                if (context.getCurrentTaskState() != TaskState.TASK_STATE_RUNNING) {
                    throw new IllegalStateException("task is not in state ready");
                }

                // create instance of that task and execute it.
                TaskInvokeable invokeable = null;
                try {
                    invokeable = context.invokeableClass.getConstructor(TaskRuntimeContext.class, Logger.class).newInstance(context, LOG);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }

                // check instance.
                if (invokeable == null) {
                    throw new IllegalStateException("invokeable == null");
                }

                context.setInvokeable(invokeable);

                try {
                    invokeable.execute();
                } catch (Exception e) {

                    LOG.error(e.getLocalizedMessage());

                    context.dispatcher.dispatchEvent(new TaskStateTransitionEvent(context.task.topologyID,
                                                                                  context.task.taskID,
                                                                                  TaskTransition.TASK_TRANSITION_FAIL));

                    return;

                } finally {
                    invokeable = null; // let the instance be collected from gc.
                }

                context.dispatcher.dispatchEvent(new TaskStateTransitionEvent(context.task.topologyID,
                                                                              context.task.taskID,
                                                                              TaskTransition.TASK_TRANSITION_FINISH));

                context = null;
            }
        }
    }

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskExecutionUnit(final int executionUnitID) {
        // sanity check.
        if (executionUnitID < 0) {
            throw new IllegalArgumentException("executionUnitID < 0");
        }

        this.executionUnitID = executionUnitID;

        this.executor = new Thread(new ExecutionUnitRunner());

        this.taskQueue = new LinkedBlockingQueue<TaskRuntimeContext>();

        this.isExecutionUnitRunning = new AtomicBoolean(false);

        this.context = null;
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = Logger.getLogger(TaskExecutionUnit.class);

    private final int executionUnitID;

    private final Thread executor;

    private final BlockingQueue<TaskRuntimeContext> taskQueue;

    private final AtomicBoolean isExecutionUnitRunning;

    private TaskRuntimeContext context;

    // ---------------------------------------------------
    // Public.
    // ---------------------------------------------------

    public void start() {
        // check preconditions.
        if (executor.isAlive()) {
            throw new IllegalStateException("executor is already running");
        }

        isExecutionUnitRunning.set(true);

        executor.start();
    }

    public void enqueueTask(final TaskRuntimeContext context) {
        // sanity check.
        if (context == null) {
            throw new IllegalArgumentException("context == null");
        }

        taskQueue.add(context);
    }

    public void stop() {
        // check preconditions.
        if (!executor.isAlive()) {
            throw new IllegalStateException("executor is not running");
        }

        isExecutionUnitRunning.set(false);
    }

    public int getNumberOfEnqueuedTasks() {
        return taskQueue.size() + (context != null ? 1 : 0);
    }

    public int getExecutionUnitID() {
        return executionUnitID;
    }
}
