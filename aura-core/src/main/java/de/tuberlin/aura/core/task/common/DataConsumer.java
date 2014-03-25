package de.tuberlin.aura.core.task.common;


import de.tuberlin.aura.core.iosystem.IOEvents;

import java.util.UUID;

public interface DataConsumer {

    public abstract IOEvents.TransferBufferEvent absorb(int gateIndex) throws InterruptedException;

    public abstract void shutdownConsumer();

    public abstract void openGate(int gateIndex);

    public abstract void closeGate(final int gateIndex);

    public abstract boolean isGateClosed(int gateIndex);

    public abstract UUID getInputTaskIDFromChannelIndex(int channelIndex);

    public abstract int getInputGateIndexFromTaskID(final UUID taskID);

    public abstract boolean isExhausted();
}
