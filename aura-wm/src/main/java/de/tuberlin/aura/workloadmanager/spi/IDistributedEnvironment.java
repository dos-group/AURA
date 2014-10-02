package de.tuberlin.aura.workloadmanager.spi;

import de.tuberlin.aura.core.topology.Topology;

import java.util.Collection;
import java.util.UUID;

/**
 *
 */
public interface IDistributedEnvironment {

    public abstract void addDataset(final Topology.DatasetNode datasetNode);

    public abstract Topology.DatasetNode getDataset(final UUID uid);

    public abstract void removeDataset(final UUID uid);

    public abstract boolean existsDataset(final UUID uid);

    public abstract <E> void addBroadcastDataset(final UUID datasetID, final Collection<E> dataset);

    public abstract <E> Collection<E> getBroadcastDataset(final UUID datasetID);
}
