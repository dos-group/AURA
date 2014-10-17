package de.tuberlin.aura.workloadmanager;

import java.util.*;
import java.util.concurrent.CountDownLatch;

import de.tuberlin.aura.core.common.utils.DeepCopy;
import de.tuberlin.aura.core.config.IConfig;
import de.tuberlin.aura.core.topology.TopologyStates;
import de.tuberlin.aura.workloadmanager.spi.ITopologyController;
import de.tuberlin.aura.workloadmanager.spi.IWorkloadManager;
import org.apache.log4j.Logger;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.common.utils.PipelineAssembler.AssemblyPipeline;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.taskmanager.common.TaskStates;
import de.tuberlin.aura.core.topology.Topology;
import de.tuberlin.aura.core.topology.Topology.AuraTopology;
import de.tuberlin.aura.core.topology.TopologyStates.TopologyState;
import de.tuberlin.aura.core.topology.TopologyStates.TopologyTransition;

public final class TopologyController extends EventDispatcher implements ITopologyController {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = Logger.getLogger(TopologyController.class);

    private final IConfig config;

    private final IWorkloadManager workloadManager;

    private final AuraTopology topology;

    private final StateMachine.FiniteStateMachine<TopologyState, TopologyTransition> topologyFSM;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public TopologyController(final IWorkloadManager workloadManager, final UUID topologyID, final AuraTopology topology, IConfig config) {
        super(true, "TopologyControllerEventDispatcher");

        // Sanity check.
        if (workloadManager == null)
            throw new IllegalArgumentException("workloadManager == null");
        if (topologyID == null)
            throw new IllegalArgumentException("topologyID == null");

        this.workloadManager = workloadManager;

        this.topology = topology;

        this.topologyFSM = createTopologyFSM();

        this.config = config;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void assembleTopology() {

        LOG.info("ASSEMBLE TOPOLOGY '" + topology.name + "'");

        // ---------------------------------------------------

        weaveInDatasets();

        // ---------------------------------------------------

        final AssemblyPipeline assemblyPipeline = new AssemblyPipeline(this.topologyFSM);

        assemblyPipeline.addPhase(new TopologyParallelizer(workloadManager.getEnvironmentManager(), config));

        assemblyPipeline.addPhase(new TopologyScheduler(workloadManager.getInfrastructureManager(), workloadManager.getEnvironmentManager()));

        assemblyPipeline.addPhase(new TopologyDeployer(workloadManager.getRPCManager()));

        assemblyPipeline.assemble(topology);

        this.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_STATE_UPDATE, new IEventHandler() {

            private int finalStateCnt = 0;

            @Override
            public void handleEvent(Event e) {
                final IOEvents.TaskControlIOEvent event = (IOEvents.TaskControlIOEvent) e;

                final Topology.ExecutionNode en = topology.executionNodeMap.get(event.getTaskID());

                // sanity check.
                if (en == null)
                    throw new IllegalStateException();

                en.setState((TaskStates.TaskState) event.getPayload());

                // It is at the moment a bit clumsy to detect the dataflow end.
                // We should introduce a dedicated "dataflow end" event...
                if (en.getState() == TaskStates.TaskState.TASK_STATE_FINISHED ||
                        en.getState() == TaskStates.TaskState.TASK_STATE_CANCELED ||
                        en.getState() == TaskStates.TaskState.TASK_STATE_FAILURE)

                    ++finalStateCnt;

                if (finalStateCnt == topology.executionNodeMap.size()) { // TODO
                    //((WorkloadManager)workloadManager).unregisterTopology(topology.topologyID);
                    //TopologyController.this.removeAllEventListener();
                    // Shutdown the event dispatcher threads used by this executingTopology controller
                    //shutdownEventDispatcher();
                    topologyFSM.joinDispatcherThread();
                    finalStateCnt = 0;
                }
            }
        });

        prepareForNextIteration();

        this.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_CLIENT_ITERATION_EVALUATION, new IEventHandler() {

            @Override
            public void handleEvent(Event event) {
                doNextIteration = (boolean)event.getPayload();
                awaitClientEvaluation.countDown();
            }
        });
    }

    public void shutdownTopologyController() {
        shutdownEventDispatcher();
        topologyFSM.shutdownEventDispatcher();
    }

    public StateMachine.FiniteStateMachine<TopologyState, TopologyTransition> getTopologyFSM() {
        return topologyFSM;
    }

    public AuraTopology getTopology() {
        return topology;
    }

    // ---------------------------------------------------

    private Set<UUID> iterationNodeSet =  new HashSet<>();

    private CountDownLatch awaitClientEvaluation = new CountDownLatch(1);

    boolean doNextIteration = false;

    public void prepareForNextIteration() {
        for (final Topology.ExecutionNode en : topology.executionNodeMap.values()) {
            iterationNodeSet.add(en.getNodeDescriptor().taskID);
        }
    }

    public synchronized void evaluateIteration(final UUID taskID) {

        if (!iterationNodeSet.remove(taskID))
            throw new IllegalStateException();

        if (iterationNodeSet.isEmpty()) {

            final IOEvents.ClientControlIOEvent iterationCycleEndEvent =
                    new IOEvents.ClientControlIOEvent(IOEvents.ControlEventType.CONTROL_EVENT_ITERATION_CYCLE_END);
            iterationCycleEndEvent.setTopologyID(topology.topologyID);
            workloadManager.getIOManager().sendEvent(topology.machineID, iterationCycleEndEvent);

            awaitClientEvaluation = new CountDownLatch(1);

            try {
                awaitClientEvaluation.await();
            } catch (InterruptedException e) {
                LOG.error(e.getMessage());
            }

            if (doNextIteration) {
                prepareForNextIteration();
                topologyFSM.dispatchEvent(new StateMachine.FSMTransitionEvent<>(TopologyStates.TopologyTransition.TOPOLOGY_TRANSITION_NEXT_ITERATION));
            }

            Topology.TopologyBreadthFirstTraverser.traverseBackwards(topology, new IVisitor<Topology.LogicalNode>() {

                @Override
                public void visit(final Topology.LogicalNode element) {

                    for (final Topology.ExecutionNode en : element.getExecutionNodes()) {

                        final IOEvents.TaskControlIOEvent nextIterationEvent =
                                new IOEvents.TaskControlIOEvent(IOEvents.ControlEventType.CONTROL_EVENT_EXECUTE_NEXT_ITERATION);

                        nextIterationEvent.setPayload(doNextIteration);
                        nextIterationEvent.setTaskID(en.getNodeDescriptor().taskID);
                        nextIterationEvent.setTopologyID(en.getNodeDescriptor().topologyID);

                        workloadManager.getIOManager().sendEvent(en.getNodeDescriptor().getMachineDescriptor(), nextIterationEvent);
                    }
                }
            });
        }
    }

    // ---------------------------------------------------

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private void weaveInDatasets() {
        Topology.TopologyBreadthFirstTraverser.traverse(topology, new IVisitor<Topology.LogicalNode>() {

            @Override
            public void visit(final Topology.LogicalNode element) {

                final Topology.LogicalNode n1 = workloadManager.getEnvironmentManager().getDataset(element.uid);

                if (n1 != null) {

                    final Topology.LogicalNode node = (Topology.LogicalNode)DeepCopy.copy(n1); //new Topology.LogicalNode(n1);

                    for (final Topology.ExecutionNode en : node.getExecutionNodes()) { // TODO: We have to change this!
                        en.getNodeDescriptor().topologyID = topology.topologyID;
                    }

                    node.isAlreadyDeployed = true;

                    node.outputs.clear();

                    node.outputs.addAll(element.outputs);

                    node.inputs.clear();

                    final List<Topology.Edge> edgeUpdates = new ArrayList<>();

                    for (final Map.Entry<Pair<String,String>, Topology.Edge> entry : topology.edges.entrySet()) {

                        final Pair<String,String> edge = entry.getKey();

                        if (edge.getFirst().equals(node.name)) {

                            final Topology.Edge e = entry.getValue();

                            edgeUpdates.add(new Topology.Edge(node, e.dstNode, e.transferType, e.edgeType));
                        }

                        if (edge.getSecond().equals(node.name)) {

                            final Topology.Edge e = entry.getValue();

                            edgeUpdates.add(new Topology.Edge(e.srcNode, node, e.transferType, e.edgeType));
                        }
                    }

                    for (final Topology.Edge e : edgeUpdates) {

                        topology.edges.put(new Pair<>(e.srcNode.name, e.dstNode.name), e);
                    }


                    topology.sourceMap.put(node.name, node);

                    topology.nodeMap.put(node.name, node);

                    topology.uidNodeMap.put(node.uid, node);


                    for (final Topology.LogicalNode n : topology.uidNodeMap.values()) {

                        if (n != node) {

                            for (int i = 0; i <  n.inputs.size(); ++i) {
                                final Topology.LogicalNode ins = n.inputs.get(i);
                                if (ins.uid.equals(node.uid))
                                    n.inputs.set(i, node);
                            }

                            for (int i = 0; i <  n.outputs.size(); ++i) {
                                final Topology.LogicalNode outs = n.outputs.get(i);
                                if (outs.uid.equals(node.uid))
                                    n.outputs.set(i, node);
                            }
                        }
                    }
                }
            }
        });
    }

    private StateMachine.FiniteStateMachine<TopologyState, TopologyTransition> createTopologyFSM() {

        final StateMachine.FSMTransitionConstraint2<TopologyState, TopologyTransition> runTransitionConstraint =
                new StateMachine.FSMTransitionConstraint2<TopologyState, TopologyTransition>(TaskStates.TaskTransition.TASK_TRANSITION_RUN) {

                    int numOfTasksToBeReady = 0;

                    @Override
                    public boolean eval(StateMachine.FSMTransitionEvent<? extends Enum<?>> event) {
                        boolean isReady = (++numOfTasksToBeReady) == topology.executionNodeMap.size();
                        if (isReady)
                            numOfTasksToBeReady = 0;
                        return isReady;
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
                        .and()
                        .addTransition(TopologyTransition.TOPOLOGY_TRANSITION_NEXT_ITERATION,
                                TopologyState.TOPOLOGY_STATE_DEPLOYED)
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

        topologyFSM.addGlobalStateListener(new StateMachine.IFSMStateAction<TopologyState, TopologyTransition>() {

            @Override
            public void stateAction(TopologyState previousState, TopologyTransition transition, TopologyState state) {
                LOG.info("CHANGE STATE OF TOPOLOGY '" + topology.name + "' [" + topology.topologyID + "] FROM " + previousState + " TO " + state
                        + "  [" + transition.toString() + "]");
            }
        });

        topologyFSM.addStateListener(TopologyState.TOPOLOGY_STATE_RUNNING, new StateMachine.IFSMStateAction<TopologyState, TopologyTransition>() {

            @Override
            public void stateAction(TopologyState previousState, TopologyTransition transition, TopologyState state) {

                Topology.TopologyBreadthFirstTraverser.traverse(topology, new IVisitor<Topology.LogicalNode>() {

                    @Override
                    public void visit(final Topology.LogicalNode element) {
                        for (final Topology.ExecutionNode en : element.getExecutionNodes()) {
                            final IOEvents.TaskControlIOEvent transitionUpdate =
                                    new IOEvents.TaskControlIOEvent(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_TRANSITION);
                            transitionUpdate.setPayload(new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_RUN));
                            transitionUpdate.setTaskID(en.getNodeDescriptor().taskID);
                            transitionUpdate.setTopologyID(en.getNodeDescriptor().topologyID);
                            workloadManager.getIOManager().sendEvent(en.getNodeDescriptor().getMachineDescriptor(), transitionUpdate);
                        }
                    }
                });
            }
        });

        topologyFSM.addStateListener(TopologyState.TOPOLOGY_STATE_FAILURE, new StateMachine.IFSMStateAction<TopologyState, TopologyTransition>() {

            @Override
            public void stateAction(TopologyState previousState, TopologyTransition transition, TopologyState state) {

                workloadManager.getIOManager().sendEvent(topology.machineID, new IOEvents.ControlIOEvent(IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FAILURE));

                Topology.TopologyBreadthFirstTraverser.traverse(topology, new IVisitor<Topology.LogicalNode>() {

                    @Override
                    public void visit(final Topology.LogicalNode element) {
                        for (final Topology.ExecutionNode en : element.getExecutionNodes()) {
                            final IOEvents.TaskControlIOEvent transitionUpdate =
                                    new IOEvents.TaskControlIOEvent(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_TRANSITION);
                            transitionUpdate.setPayload(new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_CANCEL));
                            transitionUpdate.setTaskID(en.getNodeDescriptor().taskID);
                            transitionUpdate.setTopologyID(en.getNodeDescriptor().topologyID);
                            if (en.getState().equals(TaskStates.TaskState.TASK_STATE_RUNNING)) {
                                workloadManager.getIOManager().sendEvent(en.getNodeDescriptor().getMachineDescriptor(), transitionUpdate);
                            }
                        }
                    }
                });
            }
        });

        topologyFSM.addStateListener(TopologyState.TOPOLOGY_STATE_FINISHED, new StateMachine.IFSMStateAction<TopologyState, TopologyTransition>() {

            @Override
            public void stateAction(TopologyState previousState, TopologyTransition transition, TopologyState state) {
                // Send to the examples the finish notification...
                IOEvents.ControlIOEvent event = new IOEvents.ControlIOEvent(IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FINISHED);
                event.setPayload(TopologyController.this.topology.name);
                workloadManager.getIOManager().sendEvent(topology.machineID, event);
                // Shutdown the event dispatcher threads used by this executingTopology controller
                TopologyController.this.topologyFSM.shutdownEventDispatcher();
            }
        });

        topologyFSM.addStateListener(TopologyState.ERROR, new StateMachine.IFSMStateAction<TopologyState, TopologyTransition>() {

            @Override
            public void stateAction(TopologyState previousState, TopologyTransition transition, TopologyState state) {
                throw new IllegalStateException(
                        "previousState: " + previousState.toString()
                        + ", transition: " + transition.toString()
                        + ", currentState: " + state );
            }
        });

        return topologyFSM;
    }
}
