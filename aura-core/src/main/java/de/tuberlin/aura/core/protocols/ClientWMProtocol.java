package de.tuberlin.aura.core.protocols;

import de.tuberlin.aura.core.topology.AuraDirectedGraph.AuraTopology;

public interface ClientWMProtocol {

	public void submitTopology(final AuraTopology topology);

}
