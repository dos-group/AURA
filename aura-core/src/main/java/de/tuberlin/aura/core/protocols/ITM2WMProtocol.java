package de.tuberlin.aura.core.protocols;

import java.util.Collection;
import java.util.UUID;


public interface ITM2WMProtocol {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract <E> Collection<E> getBroadcastDataset(final UUID datasetID);
}
