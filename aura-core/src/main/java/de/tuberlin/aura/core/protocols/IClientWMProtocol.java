package de.tuberlin.aura.core.protocols;

import java.util.Collection;
import java.util.UUID;

import de.tuberlin.aura.core.topology.Topology.AuraTopology;

public interface IClientWMProtocol {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract void openSession(final UUID sessionID);

    public abstract void submitTopology(final UUID sessionID, final AuraTopology topology);

    public abstract void closeSession(final UUID sessionID);

    public abstract <E> Collection<E> getDataset(final UUID uid);

    public abstract <E> void broadcastDataset(final UUID datasetID, final Collection<E> dataset);

    public abstract void assignDataset(final UUID dstDatasetID, final UUID srcDatasetID);

    //public abstract void shutdownWorkloadManager();
}
