package de.tuberlin.aura.workloadmanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.common.utils.PipelineAssembler.AssemblyPipeline;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.task.common.TaskStates;
import de.tuberlin.aura.core.topology.AuraDirectedGraph;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.topology.TopologyStates.TopologyState;
import de.tuberlin.aura.core.topology.TopologyStates.TopologyTransition;

public class TopologyController extends EventDispatcher {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(TopologyController.class);

    private final WorkloadManagerContext context;

    private final AuraTopology topology;

    private final IOManager ioManager;

    private final AssemblyPipeline assemblyPipeline;

    private final StateMachine.FiniteStateMachine<TopologyState, TopologyTransition> topologyFSM;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    /**
     * @param context
     * @param topology
     */
    public TopologyController(final WorkloadManagerContext context, final AuraTopology topology) {
        super(true, "TopologyControllerEventDispatcher");

        // sanity check.
        if (context == null)
            throw new IllegalArgumentException("context == null");
        if (topology == null)
            throw new IllegalArgumentException("topology == null");

        this.context = context;

        this.topology = topology;

        this.ioManager = context.ioManager;

        this.topologyFSM = createTopologyFSM();

        this.assemblyPipeline = new AssemblyPipeline(this.topologyFSM);

        assemblyPipeline.addPhase(new TopologyParallelizer());

        assemblyPipeline.addPhase(new TopologyScheduler(context.infrastructureManager));

        assemblyPipeline.addPhase(new TopologyDeployer(context.rpcManager));

        // TODO: No TASK_STATE_FINISHED events arrive here!
        this.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_STATE_UPDATE, new IEventHandler() {

            private int finalStateCnt = 0;

            @Override
            public void handleEvent(Event e) {
                final IOEvents.TaskControlIOEvent event = (IOEvents.TaskControlIOEvent) e;
                final AuraDirectedGraph.ExecutionNode en = topology.executionNodeMap.get(event.getTaskID());

                // sanity check.
                if (en == null)
                    throw new IllegalStateException();

                en.setState((TaskStates.TaskState) event.getPayload());

                // It is at the moment a bit clumsy to detect the processing end.
                // We should introduce a dedicated "processing end" event...
                if (en.getState() == TaskStates.TaskState.TASK_STATE_FINISHED || en.getState() == TaskStates.TaskState.TASK_STATE_CANCELED
                        || en.getState() == TaskStates.TaskState.TASK_STATE_FAILURE)
                    ++finalStateCnt;

                if (finalStateCnt == topology.executionNodeMap.size()) {
                    context.workloadManager.unregisterTopology(topology.topologyID);
                    TopologyController.this.removeAllEventListener();
                }
            }
        });
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    /**
     * @return
     */
    public AuraTopology assembleTopology() {
        LOG.info("ASSEMBLE TOPOLOGY '" + topology.name + "'");
        assemblyPipeline.assemble(topology);
        return topology;
    }

    /**
     * @return
     */
    public IEventDispatcher getTopologyFSMDispatcher() {
        return topologyFSM;
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    /**
     * @return
     */
    private StateMachine.FiniteStateMachine<TopologyState, TopologyTransition> createTopologyFSM() {

        final StateMachine.FSMTransitionConstraint2<TopologyState, TopologyTransition> runTransitionConstraint =
                new StateMachine.FSMTransitionConstraint2<TopologyState, TopologyTransition>(TaskStates.TaskTransition.TASK_TRANSITION_RUN) {

                    int numOfTasksToBeReady = 0;

                    @Override
                    public boolean eval(StateMachine.FSMTransitionEvent<? extends Enum<?>> event) {
                        return (++numOfTasksToBeReady) == topology.executionNodeMap.size();
                    }
                };

        final StateMachine.FSMTransitionConstraint2<TopologyState, TopologyTransition> finishTransitionConstraint =
                new StateMachine.FSMTransitionConstraint2<TopologyState, TopologyTransition>(TaskStates.TaskTransition.TASK_TRANSITION_FINISH) {

                    int numOfTasksToBeFinished = 0;

                    @Override
                    public boolean eval(StateMachine.FSMTransitionEvent<? extends Enum<?>> event) {
                        return (++numOfTasksToBeFinished) == topology.executionNodeMap.size();
                    }
                };


        final StateMachine.FSMTransitionConstraint2<TopologyState, TopologyTransition> cancelTransitionConstraint =
                new StateMachine.FSMTransitionConstraint2<TopologyState, TopologyTransition>(TaskStates.TaskTransition.TASK_TRANSITION_CANCEL) {

                    int numOfTasksToBeCanceled = 0;

                    @Override
                    public boolean eval(StateMachine.FSMTransitionEvent<? extends Enum<?>> event) {
                        return (++numOfTasksToBeCanceled) == topology.executionNodeMap.size();
                    }
                };

        final StateMachine.FSMTransitionConstraint2<TopologyState, TopologyTransition> failureTransitionConstraint =
                new StateMachine.FSMTransitionConstraint2<TopologyState, TopologyTransition>(TaskStates.TaskTransition.TASK_TRANSITION_FAIL) {

                    @Override
                    public boolean eval(StateMachine.FSMTransitionEvent<? extends Enum<?>> event) {
                        return true;
                    }
                };

        final StateMachine.FiniteStateMachineBuilder<TopologyState, TopologyTransition> topologyFSMBuilder =
                new StateMachine.FiniteStateMachineBuilder<>(TopologyState.class, TopologyTransition.class, TopologyState.ERROR);

        final StateMachine.FiniteStateMachine<TopologyState, TopologyTransition> topologyFSM =
                topologyFSMBuilder.defineState(TopologyState.TOPOLOGY_STATE_CREATED)
                                  .addTransition(TopologyTransition.TOPOLOGY_TRANSITION_PARALLELIZE, TopologyState.TOPOLOGY_STATE_PARALLELIZED)
                                  .defineState(TopologyState.TOPOLOGY_STATE_PARALLELIZED)
                                  .addTransition(TopologyTransition.TOPOLOGY_TRANSITION_SCHEDULE, TopologyState.TOPOLOGY_STATE_SCHEDULED)
                                  .defineState(TopologyState.TOPOLOGY_STATE_SCHEDULED)
                                  .addTransition(TopologyTransition.TOPOLOGY_TRANSITION_DEPLOY, TopologyState.TOPOLOGY_STATE_DEPLOYED)
                                  .defineState(TopologyState.TOPOLOGY_STATE_DEPLOYED)
                                  .addTransition(TopologyTransition.TOPOLOGY_TRANSITION_RUN,
                                                 TopologyState.TOPOLOGY_STATE_RUNNING,
                                                 runTransitionConstraint)
                                  .defineState(TopologyState.TOPOLOGY_STATE_RUNNING)
                                  .addTransition(TopologyTransition.TOPOLOGY_TRANSITION_FINISH,
                                                 TopologyState.TOPOLOGY_STATE_FINISHED,
                                                 finishTransitionConstraint)
                                  .and()
                                  .addTransition(TopologyTransition.TOPOLOGY_TRANSITION_CANCEL,
                                                 TopologyState.TOPOLOGY_STATE_CANCELED,
                                                 cancelTransitionConstraint)
                                  .and()
                                  .addTransition(TopologyTransition.TOPOLOGY_TRANSITION_FAIL,
                                                 TopologyState.TOPOLOGY_STATE_FAILURE,
                                                 failureTransitionConstraint)
                                  .defineState(TopologyState.TOPOLOGY_STATE_FINISHED)
                                  .noTransition()
                                  .defineState(TopologyState.TOPOLOGY_STATE_CANCELED)
                                  .noTransition()
                                  .defineState(TopologyState.TOPOLOGY_STATE_FAILURE)
                                  .noTransition()
                                  .defineState(TopologyState.ERROR)
                                  .noTransition()
                                  .setInitialState(TopologyState.TOPOLOGY_STATE_CREATED)
                                  .build();

        topologyFSM.addGlobalStateListener(new StateMachine.FSMStateAction<TopologyState, TopologyTransition>() {

            @Override
            public void stateAction(TopologyState previousState, TopologyTransition transition, TopologyState state) {
                LOG.info("CHANGE STATE OF TOPOLOGY '" + topology.name + "' [" + topology.topologyID + "] FROM " + previousState + " TO " + state
                        + "  [" + transition.toString() + "]");
            }
        });

        topologyFSM.addStateListener(TopologyState.TOPOLOGY_STATE_RUNNING, new StateMachine.FSMStateAction<TopologyState, TopologyTransition>() {

            @Override
            public void stateAction(TopologyState previousState, TopologyTransition transition, TopologyState state) {

                AuraDirectedGraph.TopologyBreadthFirstTraverser.traverse(topology, new AuraDirectedGraph.Visitor<AuraDirectedGraph.Node>() {

                    @Override
                    public void visit(final AuraDirectedGraph.Node element) {

                        for (final AuraDirectedGraph.ExecutionNode en : element.getExecutionNodes()) {

                            final IOEvents.TaskControlIOEvent transitionUpdate =
                                    new IOEvents.TaskControlIOEvent(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_TRANSITION);

                            transitionUpdate.setPayload(new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_RUN));
                            transitionUpdate.setTaskID(en.getTaskDescriptor().taskID);
                            transitionUpdate.setTopologyID(en.getTaskDescriptor().topologyID);

                            ioManager.sendEvent(en.getTaskDescriptor().getMachineDescriptor(), transitionUpdate);
                        }
                    }
                });
            }
        });

        topologyFSM.addStateListener(TopologyState.TOPOLOGY_STATE_FAILURE, new StateMachine.FSMStateAction<TopologyState, TopologyTransition>() {

            @Override
            public void stateAction(TopologyState previousState, TopologyTransition transition, TopologyState state) {

                ioManager.sendEvent(topology.machineID, new IOEvents.ControlIOEvent(IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FAILURE));

                AuraDirectedGraph.TopologyBreadthFirstTraverser.traverse(topology, new AuraDirectedGraph.Visitor<AuraDirectedGraph.Node>() {

                    @Override
                    public void visit(final AuraDirectedGraph.Node element) {
                        for (final AuraDirectedGraph.ExecutionNode en : element.getExecutionNodes()) {

                            final IOEvents.TaskControlIOEvent transitionUpdate =
                                    new IOEvents.TaskControlIOEvent(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_TRANSITION);

                            transitionUpdate.setPayload(new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_CANCEL));
                            transitionUpdate.setTaskID(en.getTaskDescriptor().taskID);
                            transitionUpdate.setTopologyID(en.getTaskDescriptor().topologyID);

                            if (en.getState().equals(TaskStates.TaskState.TASK_STATE_RUNNING)) {
                                ioManager.sendEvent(en.getTaskDescriptor().getMachineDescriptor(), transitionUpdate);
                            }
                        }
                    }
                });
            }
        });

        topologyFSM.addStateListener(TopologyState.TOPOLOGY_STATE_FINISHED, new StateMachine.FSMStateAction<TopologyState, TopologyTransition>() {

            @Override
            public void stateAction(TopologyState previousState, TopologyTransition transition, TopologyState state) {
                // Send to the examples the finish notification...
                IOEvents.ControlIOEvent event = new IOEvents.ControlIOEvent(IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED);
                event.setPayload(TopologyController.this.topology.name);

                ioManager.sendEvent(topology.machineID, event);

                // Shutdown the event dispatcher threads used by this topology controller
                shutdown();
                topologyFSM.shutdown();
            }
        });

        return topologyFSM;
    }
}
