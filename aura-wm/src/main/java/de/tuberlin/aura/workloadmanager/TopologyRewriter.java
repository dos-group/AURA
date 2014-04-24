package de.tuberlin.aura.workloadmanager;

import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.ExecutionNode;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.Node;

import java.util.List;

public class TopologyRewriter {

	public void move(final ExecutionNode en, final MachineDescriptor newMachine) {
	}

	public void scaleUp(final AuraTopology topology, final Node node, int inc) {
	}

	public void scaleDown(final AuraTopology topology, final Node node, int dec) {
	}

	public void chain(final List<Node> nodes) {
	}
}
