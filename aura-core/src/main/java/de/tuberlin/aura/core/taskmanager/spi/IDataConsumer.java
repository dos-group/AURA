package de.tuberlin.aura.core.taskmanager.spi;


import java.util.List;
import java.util.UUID;

import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.memory.spi.IAllocator;

public interface IDataConsumer {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract IOEvents.TransferBufferEvent absorb(int gateIndex) throws InterruptedException;

    public abstract IOEvents.TransferBufferEvent absorb(int gateIndex, int channelIndex) throws InterruptedException;

    public abstract void shutdown();

    public abstract void openGate(int gateIndex);

    public abstract void closeGate(final int gateIndex);

    public abstract boolean isGateClosed(int gateIndex);

    public abstract UUID getInputTaskIDFromChannelIndex(int channelIndex);

    public abstract int getInputGateIndexFromTaskID(final UUID taskID);

    public abstract boolean isExhausted();

    public abstract void bind(final List<List<Descriptors.AbstractNodeDescriptor>> inputBinding, final IAllocator allocator);

    public abstract IAllocator getAllocator();

    public abstract boolean isInputChannelExhausted(final int gateIndex, final UUID srcTaskID);

    public abstract int getChannelIndexFromTaskID(final UUID taskID);
}
