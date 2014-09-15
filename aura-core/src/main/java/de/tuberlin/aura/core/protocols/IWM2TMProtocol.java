package de.tuberlin.aura.core.protocols;

import java.util.List;
import java.util.UUID;

import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.record.Partitioner;

public interface IWM2TMProtocol {

    public abstract void installTask(final Descriptors.DeploymentDescriptor deploymentDescriptor);

    public abstract void installTasks(final List<Descriptors.DeploymentDescriptor> deploymentDescriptors);

    public abstract void addOutputBinding(final UUID taskID,
                                          final UUID topologyID,
                                          final List<List<Descriptors.AbstractNodeDescriptor>> outputBinding,
                                          final Partitioner.PartitioningStrategy partitioningStrategy,
                                          final int[][] partitioningKeys);
}
