package de.tuberlin.aura.core.task.common;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.iosystem.netty.ExecutionUnitLocalInputEventLoopGroup;
import de.tuberlin.aura.core.iosystem.netty.ExecutionUnitNetworkInputEventLoopGroup;
import de.tuberlin.aura.core.memory.MemoryManager;
import io.netty.channel.local.LocalEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

public final class TaskExecutionUnit {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutionUnit.class);

    private final int executionUnitID;

    private final Thread executorThread;

    private final BlockingQueue<TaskDriverContext> taskQueue;

    private final AtomicBoolean isExecutionUnitRunning;

    private final TaskExecutionManager executionManager;

    private TaskDriverContext currentTaskCtx;

    private final MemoryManager.BufferAllocatorGroup inputAllocator;

    private final MemoryManager.BufferAllocatorGroup outputAllocator;

    protected final DataFlowEventLoops dataFlowEventLoops;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskExecutionUnit(final TaskExecutionManager executionManager,
                             final int executionUnitID,
                             final MemoryManager.BufferAllocatorGroup inputAllocator,
                             final MemoryManager.BufferAllocatorGroup outputAllocator) {
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
        this.dataFlowEventLoops = new DataFlowEventLoops(2, 2, 2, 2);

        this.executorThread = new Thread(new ExecutionUnitRunner());

        this.taskQueue = new LinkedBlockingQueue<>();

        this.isExecutionUnitRunning = new AtomicBoolean(false);
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
     * @return
     */
    public TaskDriverContext getCurrentTaskDriverContext() {
        return currentTaskCtx;
    }

    /**
     * @return
     */
    public MemoryManager.BufferAllocatorGroup getInputAllocator() {
        return inputAllocator;
    }

    /**
     * @return
     */
    public MemoryManager.BufferAllocatorGroup getOutputAllocator() {
        return outputAllocator;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    /**
     * @param taskDriverCtx
     */
    private void unregisterTask(final TaskDriverContext taskDriverCtx) {
        LOG.trace("unregister task {} {}", taskDriverCtx.taskDescriptor.name, taskDriverCtx.taskDescriptor.taskIndex);
        executionManager.dispatchEvent(new TaskExecutionManager.TaskExecutionEvent(TaskExecutionManager.TaskExecutionEvent.EXECUTION_MANAGER_EVENT_UNREGISTER_TASK,
                                                                                   taskDriverCtx));
    }

    /**
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
                    LOG.debug("Execution Unit {} prepares execution of task {}",
                              TaskExecutionUnit.this.executionUnitID,
                              currentTaskCtx.taskDescriptor.taskID);
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }

                final CountDownLatch executeLatch = new CountDownLatch(1);

                final TaskDriverContext taskDriverCtx = currentTaskCtx;

                executorThread.setName("Execution-Unit-" + TaskExecutionUnit.this.executionUnitID + "->" + taskDriverCtx.taskDescriptor.name);

                currentTaskCtx.taskFSM.addStateListener(TaskStates.TaskState.TASK_STATE_RUNNING,
                                                        new StateMachine.FSMStateAction<TaskStates.TaskState, TaskStates.TaskTransition>() {

                                                            @Override
                                                            public void stateAction(TaskStates.TaskState previousState,
                                                                                    TaskStates.TaskTransition transition,
                                                                                    TaskStates.TaskState state) {
                                                                executeLatch.countDown();
                                                            }
                                                        });

                currentTaskCtx.taskFSM.addStateListener(TaskStates.TaskState.TASK_STATE_FINISHED,
                                                        new StateMachine.FSMStateAction<TaskStates.TaskState, TaskStates.TaskTransition>() {

                                                            @Override
                                                            public void stateAction(TaskStates.TaskState previousState,
                                                                                    TaskStates.TaskTransition transition,
                                                                                    TaskStates.TaskState state) {
                                                                try {
                                                                    taskDriverCtx.taskDriver.teardownDriver(true);
                                                                    unregisterTask(taskDriverCtx);
                                                                } catch (Throwable t) {
                                                                    LOG.error(t.getLocalizedMessage(), t);
                                                                    throw t;
                                                                }
                                                            }

                                                            @Override
                                                            public String toString() {
                                                                return "TaskStateFinished Listener (TaskExecutionUnit -> "
                                                                        + taskDriverCtx.taskDescriptor.name + " "
                                                                        + taskDriverCtx.taskDescriptor.taskIndex + ")";
                                                            }
                                                        });

                currentTaskCtx.taskFSM.addStateListener(TaskStates.TaskState.TASK_STATE_CANCELED,
                                                        new StateMachine.FSMStateAction<TaskStates.TaskState, TaskStates.TaskTransition>() {

                                                            @Override
                                                            public void stateAction(TaskStates.TaskState previousState,
                                                                                    TaskStates.TaskTransition transition,
                                                                                    TaskStates.TaskState state) {
                                                                taskDriverCtx.taskDriver.teardownDriver(false);
                                                                unregisterTask(taskDriverCtx);
                                                            }
                                                        });

                currentTaskCtx.taskFSM.addStateListener(TaskStates.TaskState.TASK_STATE_FAILURE,
                                                        new StateMachine.FSMStateAction<TaskStates.TaskState, TaskStates.TaskTransition>() {

                                                            @Override
                                                            public void stateAction(TaskStates.TaskState previousState,
                                                                                    TaskStates.TaskTransition transition,
                                                                                    TaskStates.TaskState state) {
                                                                taskDriverCtx.taskDriver.teardownDriver(false);
                                                                unregisterTask(taskDriverCtx);
                                                            }
                                                        });

                currentTaskCtx.taskDriver.startupDriver(inputAllocator, outputAllocator);

                try {
                    executeLatch.await();
                } catch (InterruptedException e) {
                    LOG.error(e.getLocalizedMessage(), e);
                }

                currentTaskCtx.taskDriver.executeDriver();
                LOG.debug("Execution Unit {} completed the execution of task {}",
                          TaskExecutionUnit.this.executionUnitID,
                          currentTaskCtx.taskDescriptor.taskID);

                // TODO: This is necessary to indicate that this execution unit is free via the
                // getNumberOfEnqueuedTasks()-method. This isn't thread safe in any way!
                currentTaskCtx = null;
            }

            LOG.debug("Terminate thread of execution unit {}", TaskExecutionUnit.this.executionUnitID);
        }
    }

    protected class DataFlowEventLoops {

        public final ExecutionUnitNetworkInputEventLoopGroup networkInputEventLoopGroup;

        public final ExecutionUnitLocalInputEventLoopGroup localInputEventLoopGroup;

        public final NioEventLoopGroup networkOutputEventLoopGroup;

        public final LocalEventLoopGroup localOutputEventLoopGroup;

        public DataFlowEventLoops(int networkInputThreads, int localInputThreads, int networkOutputThreads, int localOutputThreads) {

            DefaultThreadFactory factory = new DefaultThreadFactory("EU-" + executionUnitID + "-networkInput");
            this.networkInputEventLoopGroup = new ExecutionUnitNetworkInputEventLoopGroup(networkInputThreads, factory);

            factory = new DefaultThreadFactory("EU-" + executionUnitID + "-localInput");
            this.localInputEventLoopGroup = new ExecutionUnitLocalInputEventLoopGroup(localInputThreads, factory);

            factory = new DefaultThreadFactory("EU-" + executionUnitID + "-networkOutput");
            this.networkOutputEventLoopGroup = new NioEventLoopGroup(networkOutputThreads, factory);

            factory = new DefaultThreadFactory("EU-" + executionUnitID + "-localOutput");
            this.localOutputEventLoopGroup = new LocalEventLoopGroup(localOutputThreads, factory);
        }
    }
}
