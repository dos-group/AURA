package de.tuberlin.aura.core.taskmanager.gates;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.iosystem.DataReader;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent;
import de.tuberlin.aura.core.iosystem.queues.BufferQueue;
import de.tuberlin.aura.core.taskmanager.spi.ITaskDriver;

public final class InputGate extends AbstractGate {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(InputGate.class);

    private DataReader dataReader;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public InputGate(final ITaskDriver taskDriver, int gateIndex) {
        super(taskDriver, gateIndex, taskDriver.getBindingDescriptor().inputGateBindings.get(gateIndex).size());
    }

    // ---------------------------------------------------
    // Public.
    // ---------------------------------------------------

    public void openGate() {
        for (int i = 0; i < numChannels; ++i) {
            final UUID srcID = taskDriver.getBindingDescriptor().inputGateBindings.get(gateIndex).get(i).taskID;
            final DataIOEvent event = new DataIOEvent(IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN, srcID, taskDriver.getNodeDescriptor().taskID);
            dataReader.write(taskDriver.getNodeDescriptor().taskID, gateIndex, i, event);
        }
    }

    public void closeGate() {
        for (int i = 0; i < numChannels; ++i) {
            final UUID srcID = taskDriver.getBindingDescriptor().inputGateBindings.get(gateIndex).get(i).taskID;
            final DataIOEvent event = new DataIOEvent(IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE, srcID, taskDriver.getNodeDescriptor().taskID);
            dataReader.write(taskDriver.getNodeDescriptor().taskID, gateIndex, i, event);
        }
    }

    public BufferQueue<DataIOEvent> getInputQueue(final int channelIndex) {
        return dataReader.getInputQueue(taskDriver.getNodeDescriptor().taskID, gateIndex, channelIndex);
    }

    public void setDataReader(final DataReader dataReader) {
        this.dataReader = dataReader;
    }
}
