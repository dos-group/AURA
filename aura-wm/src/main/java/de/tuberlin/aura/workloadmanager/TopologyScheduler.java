package de.tuberlin.aura.workloadmanager;

import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.ExecutionNode;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.Node;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.TopologyBreadthFirstTraverser;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.Visitor;
import de.tuberlin.aura.workloadmanager.spi.ITopologyScheduler;

public class TopologyScheduler implements ITopologyScheduler {

    //---------------------------------------------------
    // Constructors.
    //---------------------------------------------------

    public TopologyScheduler( final InfrastructureManager infrastructureManager ) {
        // sanity check.
        if( infrastructureManager == null )
            throw new IllegalArgumentException( "infrastructureManager == null" );

        this.infrastructureManager = infrastructureManager;
    }

    //---------------------------------------------------
    // Fields.
    //---------------------------------------------------

    private InfrastructureManager infrastructureManager;

    //---------------------------------------------------
    // Public.
    //---------------------------------------------------

    @Override
    public void scheduleTopology( AuraTopology topology ) {

        // Scheduling.
        TopologyBreadthFirstTraverser.traverse( topology, new Visitor<Node>() {

            @Override
            public void visit( final Node element ) {
                for( final ExecutionNode en : element.getExecutionNodes() ) {
                    en.getTaskDescriptor().setMachineDescriptor( infrastructureManager.getMachine() );
                }
            }
        } );
    }
}
