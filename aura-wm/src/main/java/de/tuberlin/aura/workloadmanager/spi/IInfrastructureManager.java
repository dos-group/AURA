package de.tuberlin.aura.workloadmanager.spi;

import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.filesystem.InputSplit;
import de.tuberlin.aura.core.topology.Topology;

public interface IInfrastructureManager {

    public abstract int getNumberOfMachine();

    public abstract Descriptors.MachineDescriptor getNextMachine();

    public abstract InputSplit getInputSplitFromHDFSSource(final Topology.ExecutionNode executionNode);

    public abstract void registerHDFSSource(final Topology.LogicalNode node);

    public abstract void shutdownInfrastructureManager();
}
