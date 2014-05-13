package de.tuberlin.aura.taskmanager;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import de.tuberlin.aura.core.memory.BufferAllocatorGroup;
import de.tuberlin.aura.core.task.spi.ITaskExecutionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.iosystem.netty.ExecutionUnitLocalInputEventLoopGroup;
import de.tuberlin.aura.core.iosystem.netty.ExecutionUnitNetworkInputEventLoopGroup;
import de.tuberlin.aura.core.task.spi.ITaskDriver;
import de.tuberlin.aura.core.task.spi.ITaskExecutionUnit;
import de.tuberlin.aura.core.task.common.TaskStates;
import io.netty.channel.local.LocalEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

public final class TaskExecutionUnit implements ITaskExecutionUnit {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutionUnit.class);

    private final int executionUnitID;

    private final Thread executorThread;

    private final BlockingQueue<ITaskDriver> taskQueue;

    private final AtomicBoolean isExecutionUnitRunning;

    private final ITaskExecutionManager executionManager;

    private final BufferAllocatorGroup inputAllocator;

    private final BufferAllocatorGroup outputAllocator;


    private final ExecutionUnitNetworkInputEventLoopGroup networkInputELG;

    private final ExecutionUnitLocalInputEventLoopGroup localInputELG;

    private final NioEventLoopGroup networkOutputELG;

    private final LocalEventLoopGroup localOutputELG;


    private ITaskDriver currentTaskDriver;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskExecutionUnit(final ITaskExecutionManager executionManager,
                             final int executionUnitID,
                             final BufferAllocatorGroup inputAllocator,
                             final BufferAllocatorGroup outputAllocator) {
        // sanity check.
        if (executionManager == null)
            throw new IllegalArgumentException("executionManager == null");
        if (executionUnitID < 0)
            throw new IllegalArgumentException("executionUnitID < 0");
        if (inputAllocator == null)
            throw new IllegalArgumentException("inputAllocator == null");
        if (outputAllocator == null)
            throw new IllegalArgumentException("outputAllocator == null");

        this.executionManager = executionManager;

        this.executionUnitID = executionUnitID;

        this.inputAllocator = inputAllocator;

        this.outputAllocator = outputAllocator;

        // TODO: Make this configurable
        this.executorThread = new Thread(new ExecutionUnitRunner());

        this.taskQueue = new LinkedBlockingQueue<>();

        this.isExecutionUnitRunning = new AtomicBoolean(false);


        this.networkInputELG = new ExecutionUnitNetworkInputEventLoopGroup(2);

        this.localInputELG = new ExecutionUnitLocalInputEventLoopGroup(2);

        this.networkOutputELG = new NioEventLoopGroup(2);

        this.localOutputELG = new LocalEventLoopGroup(2);
    }

    // ---------------------------------------------------
    // Public Methods.
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
     * @param taskDriver
     */
    public void enqueueTask(final ITaskDriver taskDriver) {
        // sanity check.
        if (taskDriver == null) {
            throw new IllegalArgumentException("taskDriver == null");
        }
        taskQueue.add(taskDriver);
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
        return taskQueue.size() + (currentTaskDriver != null ? 1 : 0);
    }

    /**
     * @return
     */
    public ITaskDriver getCurrentTaskDriver() {
        return currentTaskDriver;
    }

    /**
     * @return
     */
    public BufferAllocatorGroup getInputAllocator() {
        return inputAllocator;
    }

    /**
     * @return
     */
    public BufferAllocatorGroup getOutputAllocator() {
        return outputAllocator;
    }

    /**
     * @return
     */
    public int getExecutionUnitID() {
        return executionUnitID;
    }

    /**
     *
     * @return
     */
    public ExecutionUnitNetworkInputEventLoopGroup getNetworkInputELG() {
        return networkInputELG;
    }

    /**
     *
     * @return
     */
    public ExecutionUnitLocalInputEventLoopGroup getLocalInputELG() {
        return localInputELG;
    }

    /**
     *
     * @return
     */
    public NioEventLoopGroup getNetworkOutputELG() {
        return networkOutputELG;
    }

    /**
     *
     * @return
     */
    public LocalEventLoopGroup getLocalOutputELG() {
        return localOutputELG;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    /**
     * @param taskDriver
     */
    private void unregisterTask(final ITaskDriver taskDriver) {
        LOG.trace("unregister task {} {}", taskDriver.getTaskDescriptor().name, taskDriver.getTaskDescriptor().taskIndex);
        executionManager.dispatchEvent(new TaskExecutionManager.TaskExecutionEvent(TaskExecutionManager.TaskExecutionEvent.EXECUTION_MANAGER_EVENT_UNREGISTER_TASK,
                                                                                   taskDriver));
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private final class ExecutionUnitRunner implements Runnable {

        @Override
        public void run() {

            while (isExecutionUnitRunning.get()) {

                try {
                    currentTaskDriver = taskQueue.take();
                    LOG.debug("Execution Unit {} prepares execution of task {}",
                            TaskExecutionUnit.this.executionUnitID,
                            currentTaskDriver.getTaskDescriptor().taskID);
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }

                final CountDownLatch executeLatch = new CountDownLatch(1);

                final ITaskDriver oldTaskDriver = currentTaskDriver;

                executorThread.setName("Execution-Unit-" + TaskExecutionUnit.this.executionUnitID + "->" + oldTaskDriver.getTaskDescriptor().name);

                currentTaskDriver.getTaskStateMachine().addStateListener(TaskStates.TaskState.TASK_STATE_RUNNING,
                        new StateMachine.FSMStateAction<TaskStates.TaskState, TaskStates.TaskTransition>() {

                            @Override
                            public void stateAction(TaskStates.TaskState previousState,
                                                    TaskStates.TaskTransition transition,
                                                    TaskStates.TaskState state) {
                                executeLatch.countDown();
                            }
                        });

                currentTaskDriver.getTaskStateMachine().addStateListener(TaskStates.TaskState.TASK_STATE_FINISHED,
                        new StateMachine.FSMStateAction<TaskStates.TaskState, TaskStates.TaskTransition>() {

                            @Override
                            public void stateAction(TaskStates.TaskState previousState,
                                                    TaskStates.TaskTransition transition,
                                                    TaskStates.TaskState state) {
                                try {
                                    oldTaskDriver.teardownDriver(true);
                                    unregisterTask(oldTaskDriver);
                                } catch (Throwable t) {
                                    LOG.error(t.getLocalizedMessage(), t);
                                    throw t;
                                }
                            }

                            @Override
                            public String toString() {
                                return "TaskStateFinished Listener (TaskExecutionUnit -> "
                                        + oldTaskDriver.getTaskDescriptor().name + " "
                                        + oldTaskDriver.getTaskDescriptor().taskIndex + ")";
                            }
                        });

                currentTaskDriver.getTaskStateMachine().addStateListener(TaskStates.TaskState.TASK_STATE_CANCELED,
                        new StateMachine.FSMStateAction<TaskStates.TaskState, TaskStates.TaskTransition>() {

                            @Override
                            public void stateAction(TaskStates.TaskState previousState,
                                                    TaskStates.TaskTransition transition,
                                                    TaskStates.TaskState state) {
                                oldTaskDriver.teardownDriver(false);
                                unregisterTask(oldTaskDriver);
                            }
                        });

                currentTaskDriver.getTaskStateMachine().addStateListener(TaskStates.TaskState.TASK_STATE_FAILURE,
                        new StateMachine.FSMStateAction<TaskStates.TaskState, TaskStates.TaskTransition>() {

                            @Override
                            public void stateAction(TaskStates.TaskState previousState,
                                                    TaskStates.TaskTransition transition,
                                                    TaskStates.TaskState state) {
                                oldTaskDriver.teardownDriver(false);
                                unregisterTask(oldTaskDriver);
                            }
                        });


                currentTaskDriver.startupDriver(inputAllocator, outputAllocator);

                try {
                    executeLatch.await();
                } catch (InterruptedException e) {
                    LOG.error(e.getLocalizedMessage(), e);
                }

                currentTaskDriver.executeDriver();
                LOG.debug("Execution Unit {} completed the execution of task {}",
                          TaskExecutionUnit.this.executionUnitID,
                          currentTaskDriver.getTaskDescriptor().taskID);

                // TODO: This is necessary to indicate that this execution unit is free via the
                // getNumberOfEnqueuedTasks()-method. This isn't thread safe in any way!
                currentTaskDriver = null;
            }

            LOG.debug("Terminate thread of execution unit {}", TaskExecutionUnit.this.executionUnitID);
        }
    }
}
