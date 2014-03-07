package de.tuberlin.aura.core.task.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskBindingDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDescriptor;
import de.tuberlin.aura.core.iosystem.BlockingBufferQueue;
import de.tuberlin.aura.core.iosystem.DataWriter;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.IOEvents.DataEventType;
import de.tuberlin.aura.core.iosystem.IOEvents.TaskStateTransitionEvent;
import de.tuberlin.aura.core.iosystem.QueueManager;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskState;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskTransition;
import de.tuberlin.aura.core.task.gates.InputGate;
import de.tuberlin.aura.core.task.gates.OutputGate;

/**
 *
 */
public final class TaskRuntimeContext {

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TaskRuntimeContext(final TaskDescriptor task,
                              final TaskBindingDescriptor taskBinding,
                              final IEventHandler handler,
                              final Class<? extends TaskInvokeable> invokeableClass) {

        // sanity check.
        if (task == null) {
            throw new IllegalArgumentException("task == null");
        }
        if (taskBinding == null) {
            throw new IllegalArgumentException("taskBinding == null");
        }
        if (handler == null) {
            throw new IllegalArgumentException("taskEventListener == null");
        }
        if (invokeableClass == null) {
            throw new IllegalArgumentException("invokeableClass == null");
        }

        this.task = task;

        this.taskBinding = taskBinding;

        this.handler = handler;

        this.dispatcher = new EventDispatcher(true);

        this.state = TaskState.TASK_STATE_NOT_CONNECTED;

        this.invokeableClass = invokeableClass;

        this.queueManager = QueueManager.newInstance(this, new BlockingBufferQueue.Factory<IOEvents.DataIOEvent>());

        if (taskBinding.inputGateBindings.size() > 0) {
            this.inputGates = new ArrayList<InputGate>(taskBinding.inputGateBindings.size());

            for (int gateIndex = 0; gateIndex < taskBinding.inputGateBindings.size(); ++gateIndex) {
                inputGates.add(new InputGate(this, gateIndex));
                // TODO: dispatch event with reference to input queue!
            }

        } else {
            this.inputGates = null;
        }

        if (taskBinding.outputGateBindings.size() > 0) {
            this.outputGates = new ArrayList<OutputGate>(taskBinding.outputGateBindings.size());
            for (int gateIndex = 0; gateIndex < taskBinding.outputGateBindings.size(); ++gateIndex) {
                outputGates.add(new OutputGate(this, gateIndex));
            }
        } else {
            this.outputGates = null;
        }

        this.taskIDToGateIndex = new HashMap<UUID, Integer>();
        this.channelIndexToTaskID = new HashMap<Integer, UUID>();

        int channelIndex = 0;
        for (final List<TaskDescriptor> inputGate : taskBinding.inputGateBindings) {
            for (final TaskDescriptor inputTask : inputGate) {
                taskIDToGateIndex.put(inputTask.taskID, channelIndex);
                channelIndexToTaskID.put(channelIndex, inputTask.taskID);
            }
            ++channelIndex;
        }

        channelIndex = 0;
        for (final List<TaskDescriptor> outputGate : taskBinding.outputGateBindings) {
            for (final TaskDescriptor outputTask : outputGate) {
                taskIDToGateIndex.put(outputTask.taskID, channelIndex);
                channelIndexToTaskID.put(channelIndex, outputTask.taskID);
            }
            ++channelIndex;
        }

        final String[] taskEvents =
                {DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED, DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED,
                        DataEventType.DATA_EVENT_BUFFER, DataEventType.DATA_EVENT_SOURCE_EXHAUSTED,
                        TaskStateTransitionEvent.TASK_STATE_TRANSITION_EVENT};


        dispatcher.addEventListener(taskEvents, handler);
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = Logger.getLogger(TaskRuntimeContext.class);

    public final Map<UUID, Integer> taskIDToGateIndex;

    public final Map<Integer, UUID> channelIndexToTaskID;

    public final TaskDescriptor task;

    public final TaskBindingDescriptor taskBinding;

    public final IEventHandler handler;

    public final IEventDispatcher dispatcher;

    public final Class<? extends TaskInvokeable> invokeableClass;

    public final List<InputGate> inputGates;

    public final List<OutputGate> outputGates;

    public final QueueManager<IOEvents.DataIOEvent> queueManager;

    private TaskState state;

    private TaskInvokeable invokeable;

    // ---------------------------------------------------
    // Public.
    // ---------------------------------------------------

    public UUID getInputTaskIDFromChannelIndex(int channelIndex) {
        return channelIndexToTaskID.get(channelIndex);
    }

    public int getInputGateIndexFromTaskID(final UUID taskID) {
        return taskIDToGateIndex.get(taskID);
    }

    public TaskState getCurrentTaskState() {
        return state;
    }

    public TaskState doTaskStateTransition(final TaskTransition transition) {
        // sanity check.
        if (transition == null) {
            throw new IllegalArgumentException("taskTransition == null");
        }

        final Map<TaskTransition, TaskState> transitionsSpace = TaskStateMachine.TASK_STATE_TRANSITION_MATRIX.get(state);
        final TaskState nextState = transitionsSpace.get(transition);
        state = nextState;
        return state;
    }

    public void setInvokeable(final TaskInvokeable invokeable) {
        // sanity check.
        if (invokeable == null) {
            throw new IllegalArgumentException("invokeable == null");
        }
        // check state condition.
        if (this.invokeable != null) {
            throw new IllegalStateException("this.invokeable != null");
        }
        if (state != TaskState.TASK_STATE_RUNNING) {
            throw new IllegalStateException("state != TaskState.TASK_STATE_RUNNING");
        }

        this.invokeable = invokeable;
    }

    public TaskInvokeable getInvokeable() {
        // check state condition.
        // if (this.invokeable == null)
        // throw new IllegalStateException("this.invokeable == null");
        // if (state != TaskState.TASK_STATE_RUNNING)
        // throw new IllegalStateException("state != TaskState.TASK_STATE_RUNNING");

        return invokeable;
    }

    public void close(boolean awaitExhaustion) {
        if (outputGates != null) {
            for (final OutputGate og : outputGates) {
                // TODO: maybe replace with event?!
                for (final DataWriter.ChannelWriter channelWriter : og.getAllChannelWriter()) {

                    // TODO: do we want to force the shutdown, meaning we discard remaining events
                    // in the queue
                    // or wait to send them out?
                    channelWriter.shutdown(awaitExhaustion);
                }
            }
        }

        taskIDToGateIndex.clear();
        channelIndexToTaskID.clear();
        dispatcher.removeAllEventListener();
    }

    @Override
    public String toString() {
        return (new StringBuilder()).append("TaskRuntimeContext = {")
                                    .append(" task = " + task + ", ")
                                    .append(" taskBinding = " + taskBinding + ", ")
                                    .append(" state = " + state.toString() + ", ")
                                    .append(" }")
                                    .toString();
    }
}
