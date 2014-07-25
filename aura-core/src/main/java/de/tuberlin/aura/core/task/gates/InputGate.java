package de.tuberlin.aura.core.task.gates;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.iosystem.DataReader;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.IOEvents.DataIOEvent;
import de.tuberlin.aura.core.iosystem.queues.BufferQueue;
import de.tuberlin.aura.core.task.spi.ITaskDriver;

public final class InputGate extends AbstractGate {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(InputGate.class);

    /** Reference to the DataReader component of the underlying I/O subsystem.*/
    private DataReader dataReader;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    /**
     * Constructor.
     * @param taskDriver The associated task driver context.
     * @param gateIndex The index of the input gate.
     */
    public InputGate(final ITaskDriver taskDriver, int gateIndex) {
        super(taskDriver, gateIndex, taskDriver.getBindingDescriptor().inputGateBindings.get(gateIndex).size());
    }

    // ---------------------------------------------------
    // Public.
    // ---------------------------------------------------

    /**
     * Sends a open gate event to all connected tasks of this input gate.
     * Data is only send by the producer tasks if their output gates are open.
     */
    public void openGate() {
        for (int i = 0; i < numChannels; ++i) {
            final UUID srcID = taskDriver.getBindingDescriptor().inputGateBindings.get(gateIndex).get(i).taskID;
            final DataIOEvent event = new DataIOEvent(IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_OPEN, srcID, taskDriver.getNodeDescriptor().taskID);
            dataReader.write(taskDriver.getNodeDescriptor().taskID, gateIndex, i, event);
        }
    }

    /**
     * Sends a close gate event to all connected tasks of this input gate.
     * The producers send no data over their channels as long the output gates are closed.
     */
    public void closeGate() {
        for (int i = 0; i < numChannels; ++i) {
            final UUID srcID = taskDriver.getBindingDescriptor().inputGateBindings.get(gateIndex).get(i).taskID;
            final DataIOEvent event = new DataIOEvent(IOEvents.DataEventType.DATA_EVENT_OUTPUT_GATE_CLOSE, srcID, taskDriver.getNodeDescriptor().taskID);
            dataReader.write(taskDriver.getNodeDescriptor().taskID, gateIndex, i, event);
        }
    }

    /**
     * Return the input queue of the selected channel.
     * @return The index of the channel and associated queue.
     */
    public BufferQueue<DataIOEvent> getInputQueue(final int channelIndex) {
        return dataReader.getInputQueue(taskDriver.getNodeDescriptor().taskID, gateIndex, channelIndex);
    }

    /**
     * Inject the data reader of underlying I/O subsystem.
     * @param dataReader Reference to the DataReader instance.
     */
    public void setDataReader(final DataReader dataReader) {
        this.dataReader = dataReader;
    }
}
