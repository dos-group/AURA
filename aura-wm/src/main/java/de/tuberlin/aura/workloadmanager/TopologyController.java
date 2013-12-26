package de.tuberlin.aura.workloadmanager;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.common.utils.PipelineAssembler.AssemblyPipeline;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.ExecutionNode;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.Node;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.TopologyBreadthFirstTraverser;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.Visitor;
import de.tuberlin.aura.core.iosystem.IOEvents.ControlEventType;
import de.tuberlin.aura.core.iosystem.IOEvents.ControlIOEvent;
import de.tuberlin.aura.core.iosystem.IOEvents.TaskStateEvent;
import de.tuberlin.aura.core.iosystem.IOEvents.TaskStateTransitionEvent;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskState;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskTransition;
import de.tuberlin.aura.workloadmanager.TopologyEvents.TopologyStateTransitionEvent;
import de.tuberlin.aura.workloadmanager.TopologyStateMachine.TopologyState;
import de.tuberlin.aura.workloadmanager.TopologyStateMachine.TopologyTransition;

public class TopologyController extends EventDispatcher {

    //---------------------------------------------------
    // Inner Classes.
    //---------------------------------------------------

    private static final class TaskStateConsensus {

        public static enum Consensus {

            CONSENSUS_ATTAINED,

            CONSENSUS_NOT_YET_ATTAINED,

            CONSENSUS_UNATTAINABLE
        }

        public TaskStateConsensus( final TaskState consensusState, final AuraTopology topology ) {
            // sanity check.
            if( consensusState == null )
                throw new IllegalArgumentException( "consensusState == null" );
            if( topology == null )
                throw new IllegalArgumentException( "topology == null" );

            this.consensusState = consensusState;
            this.taskIDs = new HashSet<UUID>( topology.executionNodeMap.keySet() );
        }

        private final Set<UUID> taskIDs;

        private final TaskState consensusState;

        public Consensus isConsensusAttained( final TaskStateEvent event ) {
            if( event.state == consensusState ) {
                taskIDs.remove( event.taskID );
            } else
                return Consensus.CONSENSUS_UNATTAINABLE;
            if( taskIDs.size() == 0 ) {
                return Consensus.CONSENSUS_ATTAINED;
            } else
                return Consensus.CONSENSUS_NOT_YET_ATTAINED;
        }
    }

    private final class TopologyEventHandler extends EventHandler {

        public TopologyEventHandler( final TopologyController topologyController ) {
            this.topologyController = topologyController;
        }

        private final TopologyController topologyController;

        private TaskStateConsensus stateConsensus = null;

        @Handle( event = TaskStateEvent.class )
        private void handleTaskStateEvent( final TaskStateEvent event ) {
            if( state == TopologyState.TOPOLOGY_STATE_SCHEDULED ||
                state == TopologyState.TOPOLOGY_STATE_DEPLOYED ) {
                if( stateConsensus == null )
                    stateConsensus = new TaskStateConsensus( TaskState.TASK_STATE_READY, topology );
                switch( stateConsensus.isConsensusAttained( event ) ) {
                    case CONSENSUS_ATTAINED: {
                        dispatchTaskEvent( new TaskStateTransitionEvent( topology.topologyID, TaskTransition.TASK_TRANSITION_RUN ) );
                        dispatchEvent( new TopologyStateTransitionEvent( TopologyTransition.TOPOLOGY_TRANSITION_RUN ) );
                        stateConsensus = null;
                    } break;
                    case CONSENSUS_NOT_YET_ATTAINED: {} break;
                    case CONSENSUS_UNATTAINABLE: {} break;
                }
            }

            if( state == TopologyState.TOPOLOGY_STATE_RUNNING ) {
                if( stateConsensus == null )
                    stateConsensus = new TaskStateConsensus( TaskState.TASK_STATE_FINISHED, topology );
                switch( stateConsensus.isConsensusAttained( event ) ) {
                    case CONSENSUS_ATTAINED: {
                        dispatchEvent( new TopologyStateTransitionEvent( TopologyTransition.TOPOLOGY_TRANSITION_FINISH ) );
                        stateConsensus = null;
                    } break;
                    case CONSENSUS_NOT_YET_ATTAINED: {} break;
                    case CONSENSUS_UNATTAINABLE: {} break;
                }
            }
        }

        @Handle( event = TopologyStateTransitionEvent.class )
        private void handleTopologyStateTransition( final TopologyStateTransitionEvent event ) {
            final TopologyState oldState = state;
            final Map<TopologyTransition,TopologyState> transitionsSpace =
                    TopologyStateMachine.TOPOLOGY_STATE_TRANSITION_MATRIX.get( state );
            final TopologyState nextState = transitionsSpace.get( event.transition );
            state = nextState;
            // Trigger state dependent actions. Realization of a classic Moore automata.
            switch( state ) {
                case TOPOLOGY_STATE_CREATED: {} break;
                case TOPOLOGY_STATE_PARALLELIZED: {} break;
                case TOPOLOGY_STATE_SCHEDULED: {} break;
                case TOPOLOGY_STATE_DEPLOYED: {} break;
                case TOPOLOGY_STATE_RUNNING: {} break;
                case TOPOLOGY_STATE_FINISHED: {
                    workloadManager.unregisterTopology( topology.topologyID );
                    topologyController.removeAllEventListener();
                } break;
                case TOPOLOGY_STATE_FAILURE: {} break;
                case TOPOLOGY_STATE_UNDEFINED: {
                    throw new IllegalStateException( "topology " + topology.name + " [" + topology.topologyID + "] from state "
                            + oldState + " to " + state + " is not defined" );
                }
            }
            LOG.info( "TOPOLOGY " + topology.name + " MAKES A TRANSITION FROM " + oldState + " TO " + state );
        }
    }

    //---------------------------------------------------
    // Constructors.
    //---------------------------------------------------

    public TopologyController( final WorkloadManager workloadManager,
                               final AuraTopology topology ) {
        super( true );

        // sanity check.
        if( workloadManager == null )
            throw new IllegalArgumentException( "workloadManager == null" );
        if( topology == null )
            throw new IllegalArgumentException( "topology == null" );

        this.workloadManager = workloadManager;

        this.topology = topology;

        this.ioManager = workloadManager.getIOManager();

        this.state = TopologyState.TOPOLOGY_STATE_CREATED;

        this.assemblyPipeline = new AssemblyPipeline( this );

        assemblyPipeline.addPhase( new TopologyParallelizer() )
                        .addPhase( new TopologyScheduler( workloadManager.getInfrastructureManager() ) )
                        .addPhase( new TopologyDeployer( workloadManager.getRPCManager() ) );

        this.eventHandler = new TopologyEventHandler( this );
        this.addEventListener( TopologyStateTransitionEvent.TOPOLOGY_STATE_TRANSITION_EVENT, eventHandler );
        this.addEventListener( ControlEventType.CONTROL_EVENT_TASK_STATE, eventHandler );
    }

    //---------------------------------------------------
    // Fields.
    //---------------------------------------------------

    private static final Logger LOG = Logger.getLogger( TopologyController.class );

    private final WorkloadManager workloadManager;

    private final AuraTopology topology;

    private final IOManager ioManager;

    private final AssemblyPipeline assemblyPipeline;

    private final TopologyEventHandler eventHandler;

    private TopologyState state;

    //---------------------------------------------------
    // Public.
    //---------------------------------------------------

    AuraTopology assembleTopology() {
        // sanity check.
        if( topology == null )
            throw new IllegalArgumentException( "topology == null" );

        LOG.info( "ASSEMBLE TOPOLOGY '" + topology.name + "'"  );
        assemblyPipeline.assemble( topology );
        return topology;
    }

    //---------------------------------------------------
    // Private.
    //---------------------------------------------------

    private void dispatchTaskEvent( final ControlIOEvent event ) {
        // sanity check.
        if( event == null )
            throw new IllegalArgumentException( "event == null" );

        TopologyBreadthFirstTraverser.traverse( topology, new Visitor<Node>() {

            @Override
            public void visit( final Node element ) {
                for( final ExecutionNode en : element.getExecutionNodes() ) {
                    ioManager.sendEvent( en.getTaskDescriptor().getMachineDescriptor(), event );
                }
            }
        } );
    }
}