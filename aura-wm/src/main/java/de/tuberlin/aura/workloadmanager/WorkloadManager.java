package de.tuberlin.aura.workloadmanager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.protocols.ClientWMProtocol;

public class WorkloadManager implements ClientWMProtocol {

    //---------------------------------------------------
    // Constructors.
    //---------------------------------------------------

    public WorkloadManager( final MachineDescriptor machine ) {
        // sanity check.
        if( machine == null )
            throw new IllegalArgumentException( "machine == null" );

        this.machine = machine;

        this.ioManager = new IOManager( this.machine );

        this.rpcManager = new RPCManager( ioManager );

        this.infrastructureManager = new InfrastructureManager();

        this.deploymentManager = new DeploymentManager( rpcManager );

        this.registeredToplogies = new ConcurrentHashMap<String,TopologyController>();

        rpcManager.registerRPCProtocolImpl( this, ClientWMProtocol.class );
    }

    //---------------------------------------------------
    // Fields.
    //---------------------------------------------------

    private static final Logger LOG = Logger.getLogger( WorkloadManager.class );

    private final MachineDescriptor machine;

    private final IOManager ioManager;

    private final RPCManager rpcManager;

    private final InfrastructureManager infrastructureManager;

    private final DeploymentManager deploymentManager;

    private final Map<String,TopologyController> registeredToplogies;

    //---------------------------------------------------
    // Public.
    //---------------------------------------------------

    @Override
    public void submitTopology( final AuraTopology topology ) {
        // sanity check.
        if( topology == null )
            throw new IllegalArgumentException( "topology == null" );

        if( registeredToplogies.containsKey( topology.name ) )
            throw new IllegalStateException( "topology already submitted" );

        LOG.info( "submit topology " + topology.name );

        final TopologyController topologyController = new TopologyController( topology, infrastructureManager );
        registeredToplogies.put( topology.name, topologyController );

        final AuraTopology assembledTopology = topologyController.assembleTopology();
        deploymentManager.deployTopology( assembledTopology );
    }

    public RPCManager getRPCManager() {
        return rpcManager;
    }

    public IOManager getIOManager() {
        return ioManager;
    }
}
