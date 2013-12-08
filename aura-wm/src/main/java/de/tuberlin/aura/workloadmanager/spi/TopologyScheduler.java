package de.tuberlin.aura.workloadmanager.spi;

import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.AuraTopology;

public interface TopologyScheduler {

	public abstract void scheduleTopology( final AuraTopology topology ); 
		
}
