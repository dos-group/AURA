package de.tuberlin.aura.core.protocols;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

import de.tuberlin.aura.core.dataflow.datasets.AbstractDataset;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.record.Partitioner;
import org.apache.hadoop.mapred.InputSplit;

public interface IWM2TMProtocol {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public abstract void installTask(final Descriptors.DeploymentDescriptor deploymentDescriptor);

    public abstract void addOutputBinding(final UUID taskID,
                                          final UUID topologyID,
                                          final List<List<Descriptors.AbstractNodeDescriptor>> outputBinding,
                                          final Partitioner.PartitioningStrategy partitioningStrategy,
                                          final int[][] partitioningKeys,
                                          final boolean isReExecutable,
                                          final AbstractDataset.DatasetType datasetType);

    public abstract <E> Collection<E> getDataset(final UUID uid);

    public abstract <E> Collection<E> getBroadcastDataset(final UUID datasetID);

    public abstract void assignDataset(final UUID dstDatasetTaskID, final UUID srcDatasetTaskID);
}
