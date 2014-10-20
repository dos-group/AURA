package de.tuberlin.aura.workloadmanager.spi;

import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.filesystem.InputSplit;
import de.tuberlin.aura.core.topology.Topology;

public interface IInfrastructureManager {

    public abstract int getNumberOfMachines();

    public abstract Descriptors.MachineDescriptor getNextMachineForTask(Topology.LogicalNode node);

    public abstract InputSplit getNextInputSplitForHDFSSource(final Topology.ExecutionNode executionNode);

    public abstract void registerHDFSSource(final Topology.LogicalNode node);

}
