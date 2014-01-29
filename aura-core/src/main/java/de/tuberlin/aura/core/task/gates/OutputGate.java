package de.tuberlin.aura.core.task.gates;

import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.iosystem.IChannelWriter;
import de.tuberlin.aura.core.iosystem.IOEvents.DataEventType;
import de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent;
import de.tuberlin.aura.core.task.common.TaskRuntimeContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

    public OutputGate(final TaskRuntimeContext context, int gateIndex) {
        super(context, gateIndex, context.taskBinding.outputGateBindings.get(gateIndex).size());

        // All channels are by default are closed.
        this.openChannelList = new ArrayList<>(Collections.nCopies(numChannels, false));

        if (numChannels > 0) {
            channelWriter = new ArrayList<>(Collections.nCopies(numChannels, (IChannelWriter) null));
        } else { // numChannels == 0
            channelWriter = null;
        }

        final EventHandler outputGateEventHandler = new OutputGateEventHandler();

        final String[] gateEvents = {
            DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN,
            DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE
        };

        context.dispatcher.addEventListener(gateEvents, outputGateEventHandler);
    }

    private static final Logger LOG = LoggerFactory.getLogger(OutputGate.class);

    private final List<Boolean> openChannelList;

    private final List<IChannelWriter> channelWriter;

    // ---------------------------------------------------
    // Public.
    // ---------------------------------------------------

    public void writeDataToChannel(final int channelIndex, final DataIOEvent data) {
        // sanity check.
        if (data == null) {
            throw new IllegalArgumentException("data == null");
        }

		/*if (!isGateOpen(channelIndex)) {
            LOG.error("channel is closed");
		}*/

        // TODO: change insert in output queue!
        channelWriter.get(channelIndex).write(data);
    }

    public List<IChannelWriter> getAllChannelWriter() {
        return Collections.unmodifiableList(channelWriter);
    }

    public void setChannelWriter(int channelIndex, final IChannelWriter channel) {
        // sanity check.
        if (channelIndex < 0) {
            throw new IllegalArgumentException("channelIndex < 0");
        }
        if (channelIndex >= numChannels) {
            throw new IllegalArgumentException("channelIndex >= numChannels");
        }
        if (channelWriter == null) {
            throw new IllegalStateException("channels == null");
        }

        channelWriter.set(channelIndex, channel);
    }

    public IChannelWriter getChannelWriter(int channelIndex) {
        // sanity check.
        if (channelIndex < 0) {
            throw new IllegalArgumentException("channelIndex < 0");
        }
        if (channelIndex >= numChannels) {
            throw new IllegalArgumentException("channelIndex >= numChannels");
        }
        if (channelWriter == null) {
            throw new IllegalStateException("channels == null");
        }

        return channelWriter.get(channelIndex);
    }

    public boolean isGateOpen(final int channelIndex) {
        return openChannelList.get(channelIndex);
    }
}
