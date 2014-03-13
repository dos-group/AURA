package de.tuberlin.aura.core.task.common;


import de.tuberlin.aura.core.iosystem.IOEvents;

import java.util.UUID;

public interface DataProducer {

    public abstract void emit(int gateIndex, int channelIndex, IOEvents.DataIOEvent event);

    public abstract void done();

    public abstract void shutdownProducer(boolean awaitExhaustion);

    public abstract UUID getOutputTaskIDFromChannelIndex(int channelIndex);

    public abstract int getOutputGateIndexFromTaskID(final UUID taskID);
}
