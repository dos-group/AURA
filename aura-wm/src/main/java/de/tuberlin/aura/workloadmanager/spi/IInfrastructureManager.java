package de.tuberlin.aura.workloadmanager.spi;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.filesystem.FileInputSplit;
import de.tuberlin.aura.core.filesystem.InputSplit;
import de.tuberlin.aura.core.topology.Topology;
import de.tuberlin.aura.workloadmanager.LocationPreference;

public interface IInfrastructureManager {

    public abstract int getNumberOfMachines();

    public abstract Descriptors.MachineDescriptor getMachine(LocationPreference locationPreference);

    public abstract List<InputSplit> registerHDFSSource(final Topology.LogicalNode node);

    public abstract List<Descriptors.MachineDescriptor> getMachinesWithInputSplit(InputSplit inputSplit);

    public abstract InputSplit getNextInputSplitForHDFSSource(final Topology.ExecutionNode executionNode);

    public abstract void shutdownInfrastructureManager();

    public abstract Map<UUID, Descriptors.MachineDescriptor> getTaskManagerMachines();

    public abstract void reclaimExecutionUnits(final Topology.AuraTopology finishedTopology);

    public abstract void reclaimExecutionUnits(final Topology.DatasetNode dataset);
}
