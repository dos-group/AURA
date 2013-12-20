package de.tuberlin.aura.workloadmanager;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.task.common.TaskEvents.TaskStateTransitionEvent;
import de.tuberlin.aura.workloadmanager.spi.ITopologyController;

public class TopologyController extends EventDispatcher implements ITopologyController {

    //---------------------------------------------------
    // Inner Classes.
    //---------------------------------------------------

    private final class TopologyEventHandler extends EventHandler {

        /*@Handle( event = TaskStateTransitionEvent.class )
        private void handleTaskReadyEvent( final TaskStateTransitionEvent event ) {
            LOG.info( "task " + event.taskID + " in state " + event.transition.toString() );
        }*/
    }

    //---------------------------------------------------
    // Constructors.
    //---------------------------------------------------

    public TopologyController( final AuraTopology topology,
                               final IOManager ioManager,
                               final InfrastructureManager infrastructureManager ) {
        super( true );

        // sanity check.
        if( topology == null )
            throw new IllegalArgumentException( "topology == null" );
        if( ioManager == null )
            throw new IllegalArgumentException( "ioManager == null" );
        if( infrastructureManager == null )
            throw new IllegalArgumentException( "infrastructureManager == null" );

        this.topology = topology;

        this.topologyParallelizer = new TopologyParallelizer();

        this.topologyScheduler = new TopologyScheduler( infrastructureManager );

        this.topologyEventHandler = new TopologyEventHandler();

        ioManager.addEventListener( TaskStateTransitionEvent.TASK_STATE_TRANSITION_EVENT, topologyEventHandler );
    }

    //---------------------------------------------------
    // Fields.
    //---------------------------------------------------

    private static final Logger LOG = Logger.getLogger( TopologyController.class );

    private final AuraTopology topology;

    private final TopologyParallelizer topologyParallelizer;

    private final TopologyScheduler topologyScheduler;

    private final TopologyEventHandler topologyEventHandler;

    //---------------------------------------------------
    // Public.
    //---------------------------------------------------

    AuraTopology assembleTopology() {
        // sanity check.
        if( topology == null )
            throw new IllegalArgumentException( "topology == null" );

        LOG.info( "assemble topology '" + topology.name + "'"  );

        topologyParallelizer.parallelizeTopology( topology );

        topologyScheduler.scheduleTopology( topology );

        return topology;
    }
}