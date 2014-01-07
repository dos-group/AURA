package de.tuberlin.aura.core.task.common;

import io.netty.channel.Channel;

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
import de.tuberlin.aura.core.iosystem.IOEvents.DataEventType;
import de.tuberlin.aura.core.iosystem.IOEvents.TaskStateTransitionEvent;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskState;
import de.tuberlin.aura.core.task.gates.InputGate;
import de.tuberlin.aura.core.task.gates.OutputGate;

/**
 *
 */
public final class TaskContext {

	// ---------------------------------------------------
	// Constructors.
	// ---------------------------------------------------

	public TaskContext(final TaskDescriptor task,
			final TaskBindingDescriptor taskBinding,
			final IEventHandler handler,
			final Class<? extends TaskInvokeable> invokeableClass) {

		// sanity check.
		if (task == null)
			throw new IllegalArgumentException("task == null");
		if (taskBinding == null)
			throw new IllegalArgumentException("taskBinding == null");
		if (handler == null)
			throw new IllegalArgumentException("taskEventListener == null");
		if (invokeableClass == null)
			throw new IllegalArgumentException("invokeableClass == null");

		this.task = task;

		this.taskBinding = taskBinding;

		this.handler = handler;

		this.dispatcher = new EventDispatcher(true);

		this.state = TaskState.TASK_STATE_NOT_CONNECTED;

		this.invokeableClass = invokeableClass;

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
			for (int gateIndex = 0; gateIndex < taskBinding.outputGateBindings.size(); ++gateIndex)
				outputGates.add(new OutputGate(this, gateIndex));
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
		{ DataEventType.DATA_EVENT_INPUT_CHANNEL_CONNECTED,
			DataEventType.DATA_EVENT_OUTPUT_CHANNEL_CONNECTED,
			DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN,
			DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE,
			DataEventType.DATA_EVENT_BUFFER,
			TaskStateTransitionEvent.TASK_STATE_TRANSITION_EVENT };

		dispatcher.addEventListener(taskEvents, handler);
	}

	// ---------------------------------------------------
	// Fields.
	// ---------------------------------------------------

	private static final Logger LOG = Logger.getLogger(TaskContext.class);

	public final Map<UUID, Integer> taskIDToGateIndex;

	public final Map<Integer, UUID> channelIndexToTaskID;

	public final TaskDescriptor task;

	public final TaskBindingDescriptor taskBinding;

	public final IEventHandler handler;

	public final IEventDispatcher dispatcher;

	public final Class<? extends TaskInvokeable> invokeableClass;

	public final List<InputGate> inputGates;

	public final List<OutputGate> outputGates;

	public TaskState state;

	// ---------------------------------------------------
	// Public.
	// ---------------------------------------------------

	@Override
	public String toString() {
		return (new StringBuilder())
			.append("TaskContext = {")
			.append(" task = " + task + ", ")
			.append(" taskBinding = " + taskBinding + ", ")
			.append(" state = " + state.toString() + ", ")
			.append(" }").toString();
	}

	public UUID getInputTaskIDFromChannelIndex(int channelIndex) {
		return channelIndexToTaskID.get(channelIndex);
	}

	public int getInputGateIndexFromTaskID(final UUID taskID) {
		return taskIDToGateIndex.get(taskID);
	}

	public void close() {
		if (outputGates != null) {
			for (final OutputGate og : outputGates) {
				for (final Channel ch : og.getAllChannels()) {
					try {
						ch.disconnect().sync();
						ch.close().sync();
						LOG.info("CLOSE CHANNEL " + ch.toString());
					} catch (InterruptedException e) {
						LOG.error(e);
					}
				}
			}
		}

		taskIDToGateIndex.clear();

		channelIndexToTaskID.clear();

		dispatcher.removeAllEventListener();
	}
}