package de.tuberlin.aura.taskmanager;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import de.tuberlin.aura.core.dataflow.datasets.AbstractDataset;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.drivers.DatasetDriver2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.memory.spi.IAllocator;
import de.tuberlin.aura.core.taskmanager.common.TaskStates;
import de.tuberlin.aura.core.taskmanager.spi.ITaskRuntime;
import de.tuberlin.aura.core.taskmanager.spi.ITaskExecutionManager;
import de.tuberlin.aura.core.taskmanager.spi.ITaskExecutionUnit;


public final class TaskExecutionUnit implements ITaskExecutionUnit {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutionUnit.class);

    private final int executionUnitID;

    private final Thread executorThread;

    private final BlockingQueue<ITaskRuntime> taskQueue;

    private final AtomicBoolean isExecutionUnitRunning;

    private final ITaskExecutionManager executionManager;

    private final IAllocator inputAllocator;

    private final IAllocator outputAllocator;

    private ITaskRuntime runtime;

    private boolean isExecutingDataset = false;

    private boolean isDatasetInitialized = false;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskExecutionUnit(final ITaskExecutionManager executionManager,
                             final int executionUnitID,
                             final IAllocator inputAllocator,
                             final IAllocator outputAllocator) {
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

        this.executorThread = new Thread(new ExecutionUnitRunner()); // TODO: Make this configurable

        this.taskQueue = new LinkedBlockingQueue<>();

        this.isExecutionUnitRunning = new AtomicBoolean(false);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void start() {
        // check preconditions.
        if (executorThread.isAlive()) {
            throw new IllegalStateException("executorThread is already running");
        }
        isExecutionUnitRunning.set(true);
        executorThread.start();
    }

    public void enqueueTask(final ITaskRuntime taskDriver) {
        // sanity check.
        if (taskDriver == null) {
            throw new IllegalArgumentException("runtime == null");
        }
        taskQueue.add(taskDriver);
    }

    public void stop() {
        // check preconditions.
        if (!executorThread.isAlive()) {
            throw new IllegalStateException("executorThread is not running");
        }
        isExecutionUnitRunning.set(false);
    }

    // ---------------------------------------------------
    // Public Getter Methods.
    // ---------------------------------------------------

    public int getNumberOfEnqueuedTasks() {
        return taskQueue.size() + (runtime != null ? 1 : 0);
    }

    public ITaskRuntime getRuntime() {
        return runtime;
    }

    public IAllocator getInputAllocator() {
        return inputAllocator;
    }

    public IAllocator getOutputAllocator() {
        return outputAllocator;
    }

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

                if (runtime == null || (runtime != null && !runtime.doNextIteration())) {

                    if (!isExecutingDataset) {

                        try {
                            runtime = taskQueue.take();
                            LOG.info("Execution Unit {} prepares execution of taskmanager {}",
                                    TaskExecutionUnit.this.executionUnitID,
                                    runtime.getNodeDescriptor().taskID);
                        } catch (InterruptedException e) {
                            throw new IllegalStateException(e);
                        }

                        executorThread.setName("Execution-Unit-" + TaskExecutionUnit.this.executionUnitID + "->" + runtime.getNodeDescriptor().name);

                        if (runtime.getNodeDescriptor() instanceof Descriptors.DatasetNodeDescriptor) {
                            isExecutingDataset = true;
                        }
                    }
                }

                final CountDownLatch executeLatch = new CountDownLatch(1);

                runtime.getTaskStateMachine().addStateListener(TaskStates.TaskState.TASK_STATE_RUNNING,
                        new StateMachine.IFSMStateAction<TaskStates.TaskState, TaskStates.TaskTransition>() {

                            @Override
                            public void stateAction(TaskStates.TaskState previousState,
                                                    TaskStates.TaskTransition transition,
                                                    TaskStates.TaskState state) {
                                executeLatch.countDown();
                            }
                        });

                if (!runtime.doNextIteration() && !isDatasetInitialized) {
                    runtime.initialize(inputAllocator, outputAllocator);
                }

                if (!isDatasetInitialized) {

                    try {
                        executeLatch.await();
                    } catch (InterruptedException e) {
                        LOG.error(e.getLocalizedMessage(), e);
                    }
                }

                if (isExecutingDataset) {
                    isDatasetInitialized = true;
                }

                boolean success = runtime.execute();

                if (runtime.getNodeDescriptor() instanceof Descriptors.InvokeableNodeDescriptor
                        || runtime.getNodeDescriptor() instanceof Descriptors.OperatorNodeDescriptor) {

                    if (runtime.doNextIteration()) {

                        runtime.getTaskStateMachine().dispatchEvent(new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_NEXT_ITERATION));

                    } else {

                        runtime.release(success);

                        if (success)
                            runtime.getTaskStateMachine().dispatchEvent(new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_FINISH));
                        else
                            runtime.getTaskStateMachine().dispatchEvent(new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_FAIL));

                        executionManager.getTaskManager().uninstallTask(runtime.getNodeDescriptor().taskID);

                        // This is necessary to indicate that this execution unit is free via the
                        // getNumberOfEnqueuedTasks()-method. This isn't thread safe in any way!
                        runtime = null;
                    }

                } else if (runtime.getNodeDescriptor() instanceof Descriptors.DatasetNodeDescriptor) {

                    if (runtime.doNextIteration()) {

                        // ----------------------------------------------

                        if (((DatasetDriver2)runtime.getInvokeable()).type == AbstractDataset.DatasetType.DATASET_ITERATION_HEAD_STATE)
                            ((DatasetDriver2)runtime.getInvokeable()).state = DatasetDriver2.DatasetState.DATASET_ITERATION_STATE;

                        else if (((DatasetDriver2)runtime.getInvokeable()).type == AbstractDataset.DatasetType.DATASET_ITERATION_TAIL_STATE)
                            ((DatasetDriver2)runtime.getInvokeable()).state = DatasetDriver2.DatasetState.DATASET_EMPTY;

                        // ----------------------------------------------

                        runtime.getTaskStateMachine().dispatchEvent(new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_NEXT_ITERATION));

                    } else {

                        if (success)
                            runtime.getTaskStateMachine().dispatchEvent(new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_FINISH));
                        else
                            runtime.getTaskStateMachine().dispatchEvent(new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_FAIL));
                    }
                }
            }
        }
    }
}
