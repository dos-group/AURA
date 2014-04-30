package de.tuberlin.aura.core.task.gates;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.iosystem.DataWriter;
import de.tuberlin.aura.core.iosystem.IOEvents.DataEventType;
import de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent;
import de.tuberlin.aura.core.task.common.DataProducer;
import de.tuberlin.aura.core.task.common.TaskDriverContext;

public final class OutputGate extends AbstractGate {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(OutputGate.class);

    private final List<Boolean> openChannelList;

    private final List<DataWriter.ChannelWriter> channelWriter;

    private final DataProducer producer;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    /**
     * @param driverContext
     * @param gateIndex
     * @param producer
     */
    public OutputGate(final TaskDriverContext driverContext, int gateIndex, final DataProducer producer) {
        super(driverContext, gateIndex, driverContext.taskBindingDescriptor.outputGateBindings.get(gateIndex).size());

        this.producer = producer;

        // All channels are by default are closed.
        this.openChannelList = new ArrayList<>(Collections.nCopies(numChannels, false));

        if (numChannels > 0) {
            channelWriter = new ArrayList<>(Collections.nCopies(numChannels, (DataWriter.ChannelWriter) null));
        } else { // numChannels == 0
            channelWriter = null;
        }

        final EventHandler outputGateEventHandler = new OutputGateEventHandler();

        driverContext.driverDispatcher.addEventListener(DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN, outputGateEventHandler);
        driverContext.driverDispatcher.addEventListener(DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE, outputGateEventHandler);
    }

    // ---------------------------------------------------
    // Public.
    // ---------------------------------------------------

    /**
     * @param channelIndex
     * @param data
     */
    public void writeDataToChannel(final int channelIndex, final DataIOEvent data) {
        // sanity check.
        if (data == null) {
            throw new IllegalArgumentException("data == null");
        }

        channelWriter.get(channelIndex).write(data);
    }

    /**
     * @return
     */
    public List<DataWriter.ChannelWriter> getAllChannelWriter() {
        return Collections.unmodifiableList(channelWriter);
    }

    /**
     * @param channelIndex
     * @param channel
     */
    public void setChannelWriter(int channelIndex, final DataWriter.ChannelWriter channel) {
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

    /**
     * @param channelIndex
     * @return
     */
    public DataWriter.ChannelWriter getChannelWriter(int channelIndex) {
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

    /**
     * @param channelIndex
     * @return
     */
    public boolean isGateOpen(final int channelIndex) {
        return openChannelList.get(channelIndex);
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private final class OutputGateEventHandler extends EventHandler {

        @Handle(event = DataIOEvent.class, type = DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN)
        private void handleOutputGateOpen(final DataIOEvent event) {
            openChannelList.set(producer.getOutputGateIndexFromTaskID(event.dstTaskID), true); // TODO:
            LOG.debug("GATE FOR TASK " + event.srcTaskID + " OPENED");
        }

        @Handle(event = DataIOEvent.class, type = DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE)
        private void handleOutputGateClose(final DataIOEvent event) {
            openChannelList.set(producer.getOutputGateIndexFromTaskID(event.dstTaskID), false); // TODO:
            LOG.debug("GATE FOR TASK " + event.srcTaskID + " CLOSED");
        }
    }
}
