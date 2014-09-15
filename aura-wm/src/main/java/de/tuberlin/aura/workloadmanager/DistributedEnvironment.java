package de.tuberlin.aura.workloadmanager;

import de.tuberlin.aura.core.topology.Topology;
import de.tuberlin.aura.workloadmanager.spi.IDistributedEnvironment;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public final class DistributedEnvironment implements IDistributedEnvironment {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private Map<UUID, Topology.DatasetNode> datasets;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DistributedEnvironment() {

        this.datasets = new ConcurrentHashMap<>();
    }

    // ---------------------------------------------------
    // Public.
    // ---------------------------------------------------

    public void addDataset(final Topology.DatasetNode datasetNode) {
        // sanity check.
        if (datasetNode == null)
            throw new IllegalArgumentException("descriptor == null");

        datasets.put(datasetNode.uid, datasetNode);
    }

    public Topology.LogicalNode getDataset(final UUID uid) {
        // sanity check.
        if (uid == null)
            throw new IllegalArgumentException("uid == null");

        return datasets.get(uid);
    }

    public void removeDataset(final UUID uid) {
        // sanity check.
        if (uid == null)
            throw new IllegalArgumentException("uid == null");

        datasets.remove(uid);
    }

    public boolean existsDataset(final UUID uid) {
        // sanity check.
        if (uid == null)
            throw new IllegalArgumentException("uid == null");

        return datasets.containsKey(uid);
    }
}