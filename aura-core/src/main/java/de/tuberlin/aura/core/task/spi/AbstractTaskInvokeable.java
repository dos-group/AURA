package de.tuberlin.aura.core.task.spi;

import java.util.UUID;

import de.tuberlin.aura.core.task.common.TaskStates;
import org.slf4j.Logger;

import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.measurement.MeasurementManager;

public abstract class AbstractTaskInvokeable implements ITaskLifecycle {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final ITaskDriver taskDriver;

    protected final IDataProducer producer;

    protected final IDataConsumer consumer;

    protected final Logger LOG;

    protected boolean isRunning;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public AbstractTaskInvokeable(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
        // sanity check.
        if (taskDriver == null)
            throw new IllegalArgumentException("taskDriver == null");
        if (producer == null)
            throw new IllegalArgumentException("producer == null");
        if (consumer == null)
            throw new IllegalArgumentException("consumer == null");
        if (LOG == null)
            throw new IllegalArgumentException("LOG == null");

        this.taskDriver = taskDriver;

        this.producer = producer;

        this.consumer = consumer;

        this.LOG = LOG;

        this.isRunning = true;

        taskDriver.getTaskStateMachine().addStateListener(TaskStates.TaskState.TASK_STATE_FINISHED,
                new StateMachine.FSMStateAction<TaskStates.TaskState, TaskStates.TaskTransition>() {

                    @Override
                    public void stateAction(TaskStates.TaskState previousState,
                                            TaskStates.TaskTransition transition,
                                            TaskStates.TaskState state) {
                        MeasurementManager.fireEvent(MeasurementManager.TASK_FINISHED + "-"
                                + taskDriver.getTaskDescriptor().taskID + "-" + taskDriver.getTaskDescriptor().name + "-"
                                + taskDriver.getTaskDescriptor().taskIndex);
                    }
                });
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void create() throws Throwable {}

    public void open() throws Throwable {}

    public void close() throws Throwable {}

    public void release() throws Throwable {}

    public UUID getTaskID(int gateIndex, int channelIndex) {
        return taskDriver.getTaskBindingDescriptor().outputGateBindings.get(gateIndex).get(channelIndex).taskID;
    }

    public void stopInvokeable() {
        isRunning = false;
    }

    // ---------------------------------------------------
    // Protected Methods.
    // ---------------------------------------------------

    protected boolean isInvokeableRunning() {
        return isRunning;
    }
}
