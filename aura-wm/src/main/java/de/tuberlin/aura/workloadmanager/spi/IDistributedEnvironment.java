package de.tuberlin.aura.workloadmanager.spi;

import de.tuberlin.aura.core.topology.Topology;

import java.util.UUID;

/**
 *
 */
public interface IDistributedEnvironment {

    public abstract void addDataset(final Topology.DatasetNode datasetNode);

    public abstract Topology.LogicalNode getDataset(final UUID uid);

    public abstract void removeDataset(final UUID uid);

    public abstract boolean existsDataset(final UUID uid);
}
