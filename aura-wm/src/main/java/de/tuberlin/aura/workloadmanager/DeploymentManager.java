package de.tuberlin.aura.workloadmanager;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.descriptors.Descriptors.TaskDeploymentDescriptor;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.ExecutionNode;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.Node;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.TopologyBreadthFirstTraverser;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.Visitor;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.protocols.WM2TMProtocol;
import de.tuberlin.aura.workloadmanager.spi.IDeploymentManager;

public class DeploymentManager implements IDeploymentManager {

    //---------------------------------------------------
    // Constructors.
    //---------------------------------------------------

    public DeploymentManager( final RPCManager rpcManager ) {
        // sanity check.
        if( rpcManager == null )
            throw new IllegalArgumentException( "rpcManager == null" );

        this.rpcManager = rpcManager;
    }

    //---------------------------------------------------
    // Fields.
    //---------------------------------------------------

    private static final Logger LOG = Logger.getLogger( DeploymentManager.class );

    private final RPCManager rpcManager;

    //---------------------------------------------------
    // Public.
    //---------------------------------------------------

    @Override
    public synchronized void deployTopology( final AuraTopology topology ) {

        // Deploying.
        TopologyBreadthFirstTraverser.traverseBackwards( topology, new Visitor<Node>() {

            @Override
            public void visit( final Node element ) {
                for( final ExecutionNode en : element.getExecutionNodes() ) {
                    final TaskDeploymentDescriptor tdd =
                            new TaskDeploymentDescriptor( en.getTaskDescriptor(),
                                                            en.getTaskBindingDescriptor() );
                    final WM2TMProtocol tmProtocol =
                            rpcManager.getRPCProtocolProxy( WM2TMProtocol.class,
                                                            en.getTaskDescriptor().getMachineDescriptor() );
                    tmProtocol.installTask( tdd );
                    LOG.info( "deploy task : " + tdd.toString() );
                }
            }
        } );
    }
}
