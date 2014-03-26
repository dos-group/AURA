package de.tuberlin.aura.core.task.gates;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.iosystem.BufferQueue;
import de.tuberlin.aura.core.iosystem.DataReader;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent;
import de.tuberlin.aura.core.task.common.TaskDriverContext;

public final class InputGate extends AbstractGate {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private DataReader channelReader;

    private static final Logger LOG = LoggerFactory.getLogger(InputGate.class);

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    /**
     * @param context
     * @param gateIndex
     */
    public InputGate(final TaskDriverContext context, int gateIndex) {
        super(context, gateIndex, context.taskBindingDescriptor.inputGateBindings.get(gateIndex).size());
    }

    // ---------------------------------------------------
    // Public.
    // ---------------------------------------------------

    /**
     *
     */
    public void openGate() {
        for (int i = 0; i < numChannels; ++i) {
            final UUID srcID = context.taskBindingDescriptor.inputGateBindings.get(gateIndex).get(i).taskID;

            final DataIOEvent event = new DataIOEvent(IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN, srcID, context.taskDescriptor.taskID);

            channelReader.write(context.taskDescriptor.taskID, gateIndex, i, event);
        }
    }

    /**
     *
     */
    public void closeGate() {
        for (int i = 0; i < numChannels; ++i) {
            final UUID srcID = context.taskBindingDescriptor.inputGateBindings.get(gateIndex).get(i).taskID;

            final DataIOEvent event = new DataIOEvent(IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE, srcID, context.taskDescriptor.taskID);

            channelReader.write(context.taskDescriptor.taskID, gateIndex, i, event);
        }
    }

    /**
     * @return
     */
    public BufferQueue<DataIOEvent> getInputQueue() {
        return channelReader.getInputQueue(context.taskDescriptor.taskID, gateIndex);
    }

    /**
     * @param channelReader
     */
    public void setChannelReader(final DataReader channelReader) {
        this.channelReader = channelReader;
    }

    /**
     * @return
     */
    public DataReader getChannelReader() {
        return channelReader;
    }
}
