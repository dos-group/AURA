package de.tuberlin.aura.core.task.spi;

import java.util.UUID;

import de.tuberlin.aura.core.task.common.TaskStates;
import org.slf4j.Logger;

import de.tuberlin.aura.core.common.statemachine.StateMachine;

public abstract class AbstractInvokeable implements IExecutionLifecycle {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final ITaskDriver driver;

    protected final IDataProducer producer;

    protected final IDataConsumer consumer;

    protected final Logger LOG;

    protected boolean isRunning;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public AbstractInvokeable(final ITaskDriver taskDriver, final IDataProducer producer, final IDataConsumer consumer, final Logger LOG) {
        // sanity check.
        if (taskDriver == null)
            throw new IllegalArgumentException("driver == null");
        if (producer == null)
            throw new IllegalArgumentException("producer == null");
        if (consumer == null)
            throw new IllegalArgumentException("consumer == null");
        if (LOG == null)
            throw new IllegalArgumentException("LOG == null");

        this.driver = taskDriver;

        this.producer = producer;

        this.consumer = consumer;

        this.LOG = LOG;

        this.isRunning = true;

        /*taskDriver.getTaskStateMachine().addStateListener(TaskStates.TaskState.TASK_STATE_FINISHED,
                new StateMachine.FSMStateAction<TaskStates.TaskState, TaskStates.TaskTransition>() {

                    @Override
                    public void stateAction(TaskStates.TaskState previousState,
                                            TaskStates.TaskTransition transition,
                                            TaskStates.TaskState state) {
                        MeasurementManager.fireEvent(MeasurementManager.TASK_FINISHED + "-"
                                + taskDriver.getNodeDescriptor().taskID + "-" + taskDriver.getNodeDescriptor().name + "-"
                                + taskDriver.getNodeDescriptor().taskIndex);
                    }
                });*/
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void create() throws Throwable {}

    public void open() throws Throwable {}

    public void close() throws Throwable {}

    public void release() throws Throwable {}

    public UUID getTaskID(int gateIndex, int channelIndex) {
        return driver.getBindingDescriptor().outputGateBindings.get(gateIndex).get(channelIndex).taskID;
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
