package de.tuberlin.aura.core.task.gates;

import io.netty.channel.Channel;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import de.tuberlin.aura.core.iosystem.IOMessages.DataChannelGateMessage;
import de.tuberlin.aura.core.iosystem.IOMessages.DataMessage;
import de.tuberlin.aura.core.task.common.TaskContext;

public final class InputGate extends AbstractGate {

	public InputGate(final TaskContext context, int gateIndex) {
		super(context, gateIndex, context.taskBinding.inputGateBindings.get(gateIndex).size());

		if (numChannels > 0) {
			inputQueue = new LinkedBlockingQueue<DataMessage>();
		} else { // numChannels == 0
			inputQueue = null;
		}
	}

	private final BlockingQueue<DataMessage> inputQueue;

	public BlockingQueue<DataMessage> getInputQueue() {
		return inputQueue;
	}

	public void addToInputQueue(final DataMessage message) {
		// sanity check.
		if (message == null)
			throw new IllegalArgumentException("message == null");

		inputQueue.add(message);
	}

	public void openGate() {
		for (int i = 0; i < numChannels; ++i) {
			final Channel ch = channels.get(i);
			final UUID srcID = context.taskBinding.inputGateBindings.get(gateIndex).get(i).uid;
			ch.writeAndFlush(new DataChannelGateMessage(srcID, context.task.uid,
				DataChannelGateMessage.DATA_CHANNEL_OUTPUT_GATE_OPEN));
		}
	}

	public void closeGate() {
		for (int i = 0; i < numChannels; ++i) {
			final Channel ch = channels.get(i);
			final UUID srcID = context.taskBinding.inputGateBindings.get(gateIndex).get(i).uid;
			ch.writeAndFlush(new DataChannelGateMessage(srcID, context.task.uid,
				DataChannelGateMessage.DATA_CHANNEL_OUTPUT_GATE_CLOSE));
		}
	}
}
