package de.tuberlin.aura.workloadmanager;

import java.util.EnumSet;
import java.util.Map;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.EventHandler;
import de.tuberlin.aura.core.common.utils.PipelineAssembler.AssemblyPipeline;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.IOEvents.ControlEventType;
import de.tuberlin.aura.core.iosystem.IOEvents.TaskStateTransitionEvent;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskState;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskTransition;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.*;
import de.tuberlin.aura.core.topology.TaskStateConsensus;
import de.tuberlin.aura.core.topology.TopologyEvents.TopologyStateTransitionEvent;
import de.tuberlin.aura.core.topology.TopologyStateMachine;
import de.tuberlin.aura.core.topology.TopologyStateMachine.TopologyState;
import de.tuberlin.aura.core.topology.TopologyStateMachine.TopologyTransition;

public class TopologyController extends EventDispatcher {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private final class TaskStateConsensusEventHandler extends EventHandler {

        private final TaskStateConsensus readyConsensus;

        private final TaskStateConsensus finishedConsensus;

        private final TaskStateConsensus failureConsensus;

        public TaskStateConsensusEventHandler() {

            this.readyConsensus = new TaskStateConsensus(EnumSet.of(TaskState.TASK_STATE_READY), EnumSet.of(TaskState.TASK_STATE_FAILURE), topology);

            this.finishedConsensus =
                    new TaskStateConsensus(EnumSet.of(TaskState.TASK_STATE_FINISHED), EnumSet.of(TaskState.TASK_STATE_FAILURE), topology);

            this.failureConsensus =
                    new TaskStateConsensus(EnumSet.of(TaskState.TASK_STATE_FINISHED, TaskState.TASK_STATE_FAILURE, TaskState.TASK_STATE_CANCELED),
                                           null,
                                           topology);
        }

        @Handle(event = IOEvents.MonitoringEvent.class, type = IOEvents.MonitoringEvent.MONITORING_TASK_STATE_EVENT)
        private synchronized void handleMonitoredTaskStateEvent(final IOEvents.MonitoringEvent event) {

            // Update state of execution node representation.
            topology.executionNodeMap.get(event.taskStateUpdate.taskID).setState(event.taskStateUpdate.nextTaskState);

            // Do some monitoring of task state transitions.
            if (topology.monitoringProperties.contains(AuraTopology.MonitoringType.TASK_MONITORING)) {
                ioManager.sendEvent(topology.machineID, new IOEvents.MonitoringEvent(event)); // We
                                                                                              // need
                                                                                              // a
                                                                                              // copy
                                                                                              // of
                                                                                              // the
                                                                                              // event....
            }

            // --------------------------------- READY PHASE ---------------------------------

            if (readyConsensus.voteSuccess(event.taskStateUpdate)) {

                // Every task is ready to run, dispatch run to all tasks.
                dispatchTaskTransitionEvent(TaskTransition.TASK_TRANSITION_RUN, null);
                // Move forward the topology state machine.
                dispatchEvent(new TopologyStateTransitionEvent(TopologyTransition.TOPOLOGY_TRANSITION_RUN));
            }

            if (readyConsensus.voteFail(event.taskStateUpdate)) {

                // Some task failed, we cancel all other tasks. At the moment this is very
                // conservative strategy,
                // because we cancel the whole job is something goes wrong.
                // TODO: add failure transitions to the task state machine.
                // dispatchTaskTransitionEvent(TaskTransition.TASK_TRANSITION_FAIL, null);

                // Some task is not ready to run, we abort.
                dispatchEvent(new TopologyStateTransitionEvent(TopologyTransition.TOPOLOGY_TRANSITION_FAIL));
            }

            // --------------------------------- FINISH PHASE ---------------------------------

            if (finishedConsensus.voteFail(event.taskStateUpdate)) {

                // Some task failed, we cancel all other tasks. At the moment this is very
                // conservative strategy,
                // because we cancel the whole job when something went wrong at the task managers.
                dispatchTaskTransitionEvent(TaskTransition.TASK_TRANSITION_CANCEL, TaskState.TASK_STATE_RUNNING);
            }

            if (finishedConsensus.voteSuccess(event.taskStateUpdate)) {

                // Every task is successfully finished, then we move on to finish the complete
                // topology.
                dispatchEvent(new TopologyStateTransitionEvent(TopologyTransition.TOPOLOGY_TRANSITION_FINISH));
            }

            // ----------------------------- FAILURE / CANCEL PHASE -------------------------------

            if (failureConsensus.voteSuccess(event.taskStateUpdate)) {

                if (!finishedConsensus.consensus()) {

                    // All tasks are finished or canceled, the topology move on to failure state.
                    dispatchEvent(new TopologyStateTransitionEvent(TopologyTransition.TOPOLOGY_TRANSITION_FAIL));
                }
            }
        }
    }

    private final class TopologyEventHandler extends EventHandler {

        private long timeOfLastStateChange = System.currentTimeMillis();

        @Handle(event = TopologyStateTransitionEvent.class)
        private void handleTopologyStateTransition(final TopologyStateTransitionEvent event) {

            final TopologyState oldState = state;
            final Map<TopologyTransition, TopologyState> transitionsSpace = TopologyStateMachine.TOPOLOGY_STATE_TRANSITION_MATRIX.get(state);
            final TopologyState nextState = transitionsSpace.get(event.transition);

            // Do some monitoring of topology state transitions.
            if (topology.monitoringProperties.contains(AuraTopology.MonitoringType.TOPOLOGY_MONITORING)) {
                final long currentTime = System.currentTimeMillis();
                final IOEvents.MonitoringEvent.TopologyStateUpdate topologyStateUpdate =
                        new IOEvents.MonitoringEvent.TopologyStateUpdate(topology.name, oldState, nextState, event.transition, currentTime
                                - timeOfLastStateChange);
                ioManager.sendEvent(topology.machineID, new IOEvents.MonitoringEvent(topology.topologyID, topologyStateUpdate));
                timeOfLastStateChange = currentTime;
            }

            state = nextState;

            // Trigger state dependent actions. Realization of a classic Moore automata.
            switch (state) {
                case TOPOLOGY_STATE_CREATED: {}
                    break;
                case TOPOLOGY_STATE_PARALLELIZED: {
                    final TaskStateConsensusEventHandler tsceh = new TaskStateConsensusEventHandler();
                    TopologyController.this.addEventListener(IOEvents.MonitoringEvent.MONITORING_TASK_STATE_EVENT, tsceh);
                }
                    break;
                case TOPOLOGY_STATE_SCHEDULED: {}
                    break;
                case TOPOLOGY_STATE_DEPLOYED: {}
                    break;
                case TOPOLOGY_STATE_RUNNING: {}
                    break;
                case TOPOLOGY_STATE_FINISHED: {
                    workloadManager.unregisterTopology(topology.topologyID);
                    TopologyController.this.removeAllEventListener();
                }
                    break;
                case TOPOLOGY_STATE_FAILURE: {
                    workloadManager.unregisterTopology(topology.topologyID);
                    TopologyController.this.removeAllEventListener();
                }
                    break;
                case TOPOLOGY_STATE_UNDEFINED: {
                    throw new IllegalStateException("topology " + topology.name + " [" + topology.topologyID + "] from state " + oldState + " to "
                            + state + " is not defined" + " [" + event.transition + "]");
                }
            }
            LOG.info("TOPOLOGY " + topology.name + " MAKES A TRANSITION FROM " + oldState + " TO " + state + " [" + event.transition + "]");
        }
    }

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TopologyController(final WorkloadManager workloadManager, final AuraTopology topology) {
        super(true);

        // sanity check.
        if (workloadManager == null)
            throw new IllegalArgumentException("workloadManager == null");
        if (topology == null)
            throw new IllegalArgumentException("topology == null");

        this.workloadManager = workloadManager;
        this.topology = topology;
        this.ioManager = workloadManager.getIOManager();
        this.state = TopologyState.TOPOLOGY_STATE_CREATED;
        this.assemblyPipeline = new AssemblyPipeline(this);

        assemblyPipeline.addPhase(new TopologyParallelizer())
                        .addPhase(new TopologyScheduler(workloadManager.getInfrastructureManager()))
                        .addPhase(new TopologyDeployer(workloadManager.getRPCManager()));

        this.eventHandler = new TopologyEventHandler();
        this.addEventListener(TopologyStateTransitionEvent.TOPOLOGY_STATE_TRANSITION_EVENT, eventHandler);
        this.addEventListener(ControlEventType.CONTROL_EVENT_TASK_STATE, eventHandler);
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = Logger.getLogger(TopologyController.class);

    private final WorkloadManager workloadManager;

    private final AuraTopology topology;

    private final IOManager ioManager;

    private final AssemblyPipeline assemblyPipeline;

    private final TopologyEventHandler eventHandler;

    private TopologyState state;

    // ---------------------------------------------------
    // Public.
    // ---------------------------------------------------

    AuraTopology assembleTopology() {
        LOG.info("ASSEMBLE TOPOLOGY '" + topology.name + "'");
        assemblyPipeline.assemble(topology);
        return topology;
    }

    // ---------------------------------------------------
    // Private.
    // ---------------------------------------------------

    private void dispatchTaskTransitionEvent(final TaskTransition transition, final TaskState currentState) {
        // sanity check.
        if (transition == null)
            throw new IllegalArgumentException("event == null");

        TopologyBreadthFirstTraverser.traverse(topology, new Visitor<Node>() {

            @Override
            public void visit(final Node element) {
                for (final ExecutionNode en : element.getExecutionNodes()) {
                    final TaskStateTransitionEvent event =
                            new TaskStateTransitionEvent(topology.topologyID, en.getTaskDescriptor().taskID, transition);
                    if (currentState == null || en.getState().equals(currentState)) {
                        ioManager.sendEvent(en.getTaskDescriptor().getMachineDescriptor(), event);
                    }
                }
            }
        });
    }
}
