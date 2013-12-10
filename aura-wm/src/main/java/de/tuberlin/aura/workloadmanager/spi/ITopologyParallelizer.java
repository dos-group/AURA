package de.tuberlin.aura.workloadmanager.spi;

import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.AuraTopology;

public interface ITopologyParallelizer {

	public abstract void parallelizeTopology( final AuraTopology topology ); 
	
}
