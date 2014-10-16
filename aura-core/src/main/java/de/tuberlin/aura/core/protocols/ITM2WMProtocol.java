package de.tuberlin.aura.core.protocols;

import de.tuberlin.aura.core.filesystem.InputSplit;

import java.util.Collection;
import java.util.UUID;


public interface ITM2WMProtocol {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract <E> Collection<E> getBroadcastDataset(final UUID datasetID);

    public abstract InputSplit requestNextInputSplit(final UUID topologyID, final UUID taskID, final int sequenceNumber);

    public abstract void doNextIteration(final UUID topologyID, final UUID taskID);
}
