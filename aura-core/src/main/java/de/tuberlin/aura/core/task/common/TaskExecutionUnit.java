package de.tuberlin.aura.core.task.common;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.iosystem.IOEvents;
import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public final class TaskExecutionUnit {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = Logger.getLogger(TaskExecutionUnit.class);

    private final int executionUnitID;

    private final Thread executorThread;

    private final BlockingQueue<TaskDriverContext> taskQueue;

    private final AtomicBoolean isExecutionUnitRunning;

    private final TaskExecutionManager executionManager;

    private TaskDriverContext currentTaskCtx;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskExecutionUnit(final TaskExecutionManager executionManager, final int executionUnitID) {
        // sanity check.
        if (executionManager == null)
            throw new IllegalArgumentException("executionManager == null");
        if (executionUnitID < 0) {
            throw new IllegalArgumentException("executionUnitID < 0");
        }

        this.executionManager = executionManager;

        this.executionUnitID = executionUnitID;

        this.executorThread = new Thread(new ExecutionUnitRunner());

        this.taskQueue = new LinkedBlockingQueue<TaskDriverContext>();

        this.isExecutionUnitRunning = new AtomicBoolean(false);
    }

    // ---------------------------------------------------
    // Public.
    // ---------------------------------------------------

    /**
     *
     */
    public void start() {
        // check preconditions.
        if (executorThread.isAlive()) {
            throw new IllegalStateException("executorThread is already running");
        }
        isExecutionUnitRunning.set(true);
        executorThread.start();
    }

    /**
     * @param context
     */
    public void enqueueTask(final TaskDriverContext context) {
        // sanity check.
        if (context == null) {
            throw new IllegalArgumentException("currentTaskCtx == null");
        }
        taskQueue.add(context);
    }

    /**
     *
     */
    public void stop() {
        // check preconditions.
        if (!executorThread.isAlive()) {
            throw new IllegalStateException("executorThread is not running");
        }
        isExecutionUnitRunning.set(false);
    }

    /**
     * @return
     */
    public int getNumberOfEnqueuedTasks() {
        return taskQueue.size() + (currentTaskCtx != null ? 1 : 0);
    }

    /**
     *
     * @return
     */
    public int getExecutionUnitID() {
        return executionUnitID;
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private final class ExecutionUnitRunner implements Runnable {

        @Override
        public void run() {

            while (isExecutionUnitRunning.get()) {

                try {
                    currentTaskCtx = taskQueue.take();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }

                final CountDownLatch executeLatch = new CountDownLatch(1);

                final TaskDriverContext taskDriverCtx = currentTaskCtx;

                currentTaskCtx.driverDispatcher.addEventListener(IOEvents.TaskStateTransitionEvent.TASK_STATE_TRANSITION_EVENT,
                        new IEventHandler() {

                            @Override
                            public void handleEvent(Event event) {

                                final IOEvents.TaskStateTransitionEvent transitionEvent =
                                        (IOEvents.TaskStateTransitionEvent) event;

                                switch (transitionEvent.transition) {

                                    case TASK_TRANSITION_RUN: {
                                        executeLatch.countDown();
                                    }
                                    break;

                                    case TASK_TRANSITION_FINISH: {
                                        taskDriverCtx.taskDriver.teardownDriver(true);
                                        unregisterTask();
                                    }
                                    break;

                                    case TASK_TRANSITION_CANCEL: {
                                        taskDriverCtx.taskDriver.teardownDriver(false);
                                        unregisterTask();
                                    }
                                    break;

                                    case TASK_TRANSITION_FAIL: {
                                        taskDriverCtx.taskDriver.teardownDriver(false);
                                        unregisterTask();
                                    }
                                    break;

                                    default: {
                                    }
                                }
                            }

                            private void unregisterTask() {
                                executionManager.dispatchEvent(
                                        new TaskExecutionManager.TaskExecutionEvent(
                                                TaskExecutionManager.TaskExecutionEvent.EXECUTION_MANAGER_EVENT_UNREGISTER_TASK,
                                                taskDriverCtx
                                        )
                                );
                            }
                        }
                );

                currentTaskCtx.taskDriver.startupDriver();

                try {
                    executeLatch.await();
                } catch (InterruptedException e) {
                    LOG.error(e);
                }

                currentTaskCtx.taskDriver.executeDriver();
            }
        }
    }
}