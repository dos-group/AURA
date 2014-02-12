package de.tuberlin.aura.core.task.gates;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.iosystem.BufferQueue;
import de.tuberlin.aura.core.iosystem.DataReader;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent;
import de.tuberlin.aura.core.task.common.TaskRuntimeContext;

public final class InputGate extends AbstractGate {

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public InputGate(final TaskRuntimeContext context, int gateIndex) {
        super(context, gateIndex, context.taskBinding.inputGateBindings.get(gateIndex).size());
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private DataReader channelReader;

    private static final Logger LOG = LoggerFactory.getLogger(InputGate.class);

    // ---------------------------------------------------
    // Public.
    // ---------------------------------------------------

    public void setChannelReader(final DataReader channelReader) {
        this.channelReader = channelReader;
    }

    public DataReader getChannelReader() {
        return channelReader;
    }

    public void openGate() {
        for (int i = 0; i < numChannels; ++i) {
            final UUID srcID = context.taskBinding.inputGateBindings.get(gateIndex).get(i).taskID;
            channelReader.write(context.task.taskID, gateIndex, i, new DataIOEvent(IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN,
                                                                                   srcID,
                                                                                   context.task.taskID));
        }
    }

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public void closeGate() {
        for (int i = 0; i < numChannels; ++i) {
            final UUID srcID = context.taskBinding.inputGateBindings.get(gateIndex).get(i).taskID;
            channelReader.write(context.task.taskID, gateIndex, i, new DataIOEvent(IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE,
                                                                                   srcID,
                                                                                   context.task.taskID));
        }

    }

    public BufferQueue<DataIOEvent> getInputQueue() {
        return channelReader.getInputQueue(context.task.taskID, gateIndex);
    }
}
