package de.tuberlin.aura.core.task.common;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.memory.MemoryManager;
import io.netty.channel.local.LocalEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

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
        this.dataFlowEventLoops = new DataFlowEventLoops(2, 2);

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
        LOG.debug("unregister task {} {}", taskDriverCtx.taskDescriptor.name, taskDriverCtx.taskDescriptor.taskIndex);
        // executionManager.dispatchEvent(new
        // TaskExecutionManager.TaskExecutionEvent(TaskExecutionManager.TaskExecutionEvent.EXECUTION_MANAGER_EVENT_UNREGISTER_TASK,
        // taskDriverCtx));
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
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }

                final CountDownLatch executeLatch = new CountDownLatch(1);

                final TaskDriverContext taskDriverCtx = currentTaskCtx;

                executorThread.setName(taskDriverCtx.taskDescriptor.name + "-Execution-Unit-Thread");


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

                                                                // TODO [Christian -> Deadlock
                                                                // test]: Remove this!
                                                                try {
                                                                    LOG.debug("Task State Finished Received in TaskExecutionUnit");

                                                                    taskDriverCtx.taskDriver.teardownDriver(true);
                                                                    unregisterTask(taskDriverCtx);
                                                                } catch (Throwable e) {
                                                                    LOG.error(e.getLocalizedMessage(), e);
                                                                    throw e;
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
            }
        }
    }

    protected class DataFlowEventLoops {

        public final NioEventLoopGroup networkInputEventLoopGroup;

        public final LocalEventLoopGroup localInputEventLoopGroup;

        public DataFlowEventLoops(int networkInputThreads, int localInputThreads) {
            this.networkInputEventLoopGroup = new NioEventLoopGroup(networkInputThreads);
            this.localInputEventLoopGroup = new LocalEventLoopGroup(localInputThreads);
        }
    }
}
