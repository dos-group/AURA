package de.tuberlin.aura.workloadmanager;

import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.workloadmanager.spi.ITopologyController;

public class TopologyController extends EventDispatcher implements ITopologyController {

    //---------------------------------------------------
    // Constructors.
    //---------------------------------------------------

    public TopologyController( final AuraTopology topology,
                               final InfrastructureManager infrastructureManager ) {
        super( true );

        // sanity check.
        if( topology == null )
            throw new IllegalArgumentException( "topology == null" );
        if( infrastructureManager == null )
            throw new IllegalArgumentException( "infrastructureManager == null" );

        this.topology = topology;

        //this.topologyState = TopologyState.TOPOLOGY_STATE_CREATED;

        this.topologyParallelizer = new TopologyParallelizer();

        this.topologyScheduler = new TopologyScheduler( infrastructureManager );
    }

    //---------------------------------------------------
    // Fields.
    //---------------------------------------------------

    private final AuraTopology topology;

    //private final TopologyState topologyState;

    private final TopologyParallelizer topologyParallelizer;

    private final TopologyScheduler topologyScheduler;

    //---------------------------------------------------
    // Public.
    //---------------------------------------------------

    AuraTopology assembleTopology() {
        // sanity check.
        if( topology == null )
            throw new IllegalArgumentException( "topology == null" );

        topologyParallelizer.parallelizeTopology( topology );

        topologyScheduler.scheduleTopology( topology );

        return topology;
    }
}