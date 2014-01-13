package de.tuberlin.aura.core.task.gates;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.iosystem.IOEvents.DataEventType;
import de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent;
import de.tuberlin.aura.core.task.common.TaskContext;

public final class OutputGate extends AbstractGate {

	// TODO: wire it together with the Andi's send mechanism!

	// ---------------------------------------------------
	// Inner Classes.
	// ---------------------------------------------------

	private final class OutputGateEventHandler extends EventHandler {

		@Handle(event = DataIOEvent.class, type = DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN)
		private void handleOutputGateOpen(final DataIOEvent event) {
			openChannelList.set(context.getInputGateIndexFromTaskID(event.dstTaskID), true);
			LOG.info("GATE FOR TASK " + event.srcTaskID + " OPENED");
		}

		@Handle(event = DataIOEvent.class, type = DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE)
		private void handleOutputGateClose(final DataIOEvent event) {
			openChannelList.set(context.getInputGateIndexFromTaskID(event.dstTaskID), false);
			LOG.info("GATE FOR TASK " + event.srcTaskID + " CLOSED");
		}
	}

	// ---------------------------------------------------
	// Constructors.
	// ---------------------------------------------------

	public OutputGate(final TaskContext context, int gateIndex) {
		super(context, gateIndex, context.taskBinding.outputGateBindings.get(gateIndex).size());

		// All channels are by default are closed.
		this.openChannelList = new ArrayList<Boolean>(Collections.nCopies(numChannels, false));

		final EventHandler outputGateEventHandler = new OutputGateEventHandler();

		final String[] gateEvents = {
			DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN,
			DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE
		};

		context.dispatcher.addEventListener(gateEvents, outputGateEventHandler);
	}

	// ---------------------------------------------------
	// Fields.
	// ---------------------------------------------------

	private static final Logger LOG = Logger.getLogger(OutputGate.class);

	private final List<Boolean> openChannelList;

	// ---------------------------------------------------
	// Public.
	// ---------------------------------------------------

	public void writeDataToChannel(final int channelIndex, final DataIOEvent data) {
		// sanity check.
		if (data == null)
			throw new IllegalArgumentException("data == null");

		if (!isGateOpen(channelIndex)) {
			LOG.error("channel is closed");
		}

		// TODO: change insert in output queue!
		getChannel(channelIndex).writeAndFlush(data);
	}

	public boolean isGateOpen(final int channelIndex) {
		return openChannelList.get(channelIndex);
	}
}
