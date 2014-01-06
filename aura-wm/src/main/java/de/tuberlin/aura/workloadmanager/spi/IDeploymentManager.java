package de.tuberlin.aura.workloadmanager.spi;

import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.AuraTopology;

public interface IDeploymentManager {

	public abstract void deployTopology(final AuraTopology topology);

}
