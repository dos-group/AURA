package de.tuberlin.aura.core.task.common;

import java.util.UUID;

import org.slf4j.Logger;

import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.statistic.MeasurementManager;

public abstract class TaskInvokeable implements TaskLifecycle {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    protected final TaskDriverContext driverContext;

    protected final DataProducer producer;

    protected final DataConsumer consumer;

    protected final Logger LOG;

    protected boolean isRunning;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskInvokeable(final TaskDriverContext driverContext, final DataProducer producer, final DataConsumer consumer, final Logger LOG) {
        // sanity check.
        if (driverContext == null)
            throw new IllegalArgumentException("driverContext == null");
        if (producer == null)
            throw new IllegalArgumentException("producer == null");
        if (consumer == null)
            throw new IllegalArgumentException("consumer == null");
        if (LOG == null)
            throw new IllegalArgumentException("LOG == null");

        this.driverContext = driverContext;

        this.producer = producer;

        this.consumer = consumer;

        this.LOG = LOG;

        this.isRunning = true;

        driverContext.taskFSM.addStateListener(TaskStates.TaskState.TASK_STATE_FINISHED,
                                               new StateMachine.FSMStateAction<TaskStates.TaskState, TaskStates.TaskTransition>() {

                                                   @Override
                                                   public void stateAction(TaskStates.TaskState previousState,
                                                                           TaskStates.TaskTransition transition,
                                                                           TaskStates.TaskState state) {
                                                       MeasurementManager.fireEvent(MeasurementManager.TASK_FINISHED + "-"
                                                               + driverContext.taskDescriptor.taskID + "-" + driverContext.taskDescriptor.name + "-"
                                                               + driverContext.taskDescriptor.taskIndex);
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
        return driverContext.taskBindingDescriptor.outputGateBindings.get(gateIndex).get(channelIndex).taskID;
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
