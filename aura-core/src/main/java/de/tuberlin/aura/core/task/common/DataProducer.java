package de.tuberlin.aura.core.task.common;


import java.util.UUID;

import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.BufferCallback;
import de.tuberlin.aura.core.memory.MemoryView;

public interface DataProducer {

    public abstract void emit(int gateIndex, int channelIndex, IOEvents.DataIOEvent event);

    public abstract void done();

    public abstract void shutdownProducer(boolean awaitExhaustion);

    public abstract UUID getOutputTaskIDFromChannelIndex(int channelIndex);

    public abstract int getOutputGateIndexFromTaskID(final UUID taskID);

    public abstract MemoryView alloc(BufferCallback callback);

    public abstract MemoryView allocBlocking() throws InterruptedException;
}
