package de.tuberlin.aura.core.task.spi;


import java.util.List;
import java.util.UUID;

import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.MemoryView;
import de.tuberlin.aura.core.memory.spi.IAllocator;

public interface IDataProducer {

    public abstract void emit(int gateIndex, int channelIndex, IOEvents.DataIOEvent event);

    public abstract void done();

    public abstract void shutdownProducer(boolean awaitExhaustion);

    public abstract UUID getOutputTaskIDFromChannelIndex(int channelIndex);

    public abstract int getOutputGateIndexFromTaskID(final UUID taskID);

    public abstract MemoryView alloc();


    public abstract void store(final MemoryView buffer);

    public abstract boolean hasStoredBuffers();

    public abstract AbstractInvokeable getStorage();

    /*public abstract void emitStoredBuffers(final int gateIdx);*/


    public abstract void bind(final List<List<Descriptors.AbstractNodeDescriptor>> outputBinding, final IAllocator allocator);

    public abstract IAllocator getAllocator();
}
