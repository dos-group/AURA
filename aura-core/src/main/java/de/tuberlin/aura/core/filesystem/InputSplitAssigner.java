package de.tuberlin.aura.core.filesystem;

import de.tuberlin.aura.core.topology.Topology;

public interface InputSplitAssigner {

    public InputSplit getNextInputSplit(final Topology.ExecutionNode exNode);
}