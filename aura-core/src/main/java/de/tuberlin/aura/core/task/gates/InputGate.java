package de.tuberlin.aura.core.task.gates;

import io.netty.channel.Channel;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import de.tuberlin.aura.core.iosystem.IOEvents.DataEventType;
import de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent;
import de.tuberlin.aura.core.task.common.TaskContext;

public final class InputGate extends AbstractGate {

	// ---------------------------------------------------
	// Constructors.
	// ---------------------------------------------------

	public InputGate(final TaskContext context, int gateIndex) {
		super(context, gateIndex, context.taskBinding.inputGateBindings.get(gateIndex).size());

		if (numChannels > 0) {
			inputQueue = new LinkedBlockingQueue<DataIOEvent>();
		} else { // numChannels == 0
			inputQueue = null;
		}
	}

	// ---------------------------------------------------
	// Fields.
	// ---------------------------------------------------

	private final BlockingQueue<DataIOEvent> inputQueue;

	// ---------------------------------------------------
	// Public.
	// ---------------------------------------------------

	public void addToInputQueue(final DataIOEvent dataEvent) {
		// sanity check.
		if (dataEvent == null)
			throw new IllegalArgumentException("message == null");

		inputQueue.add(dataEvent);
	}

	public void openGate() {
		for (int i = 0; i < numChannels; ++i) {
			final Channel ch = channels.get(i);
			final UUID srcID = context.taskBinding.inputGateBindings.get(gateIndex).get(i).taskID;
			ch.writeAndFlush(new DataIOEvent(DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN, srcID, context.task.taskID));
		}
	}

	public void closeGate() {
		for (int i = 0; i < numChannels; ++i) {
			final Channel ch = channels.get(i);
			final UUID srcID = context.taskBinding.inputGateBindings.get(gateIndex).get(i).taskID;
			ch.writeAndFlush(new DataIOEvent(DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE, srcID, context.task.taskID));
		}
	}

	public BlockingQueue<DataIOEvent> getInputQueue() {
		return inputQueue;
	}
}
