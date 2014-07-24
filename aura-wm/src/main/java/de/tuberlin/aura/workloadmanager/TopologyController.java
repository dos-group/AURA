package de.tuberlin.aura.workloadmanager;

import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.tuberlin.aura.core.topology.TopologyStates;
import org.apache.log4j.Logger;

import de.tuberlin.aura.core.common.eventsystem.Event;
import de.tuberlin.aura.core.common.eventsystem.EventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;
import de.tuberlin.aura.core.common.eventsystem.IEventHandler;
import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.common.utils.PipelineAssembler.AssemblyPipeline;
import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.task.common.TaskStates;
import de.tuberlin.aura.core.topology.Topology;
import de.tuberlin.aura.core.topology.Topology.AuraTopology;
import de.tuberlin.aura.core.topology.TopologyStates.TopologyState;
import de.tuberlin.aura.core.topology.TopologyStates.TopologyTransition;

public final class TopologyController extends EventDispatcher {

    private final class TopologyContainer {

        private List<AuraTopology> executedTopologies = new ArrayList<AuraTopology>();

        private AuraTopology executingTopology = null;

        private Queue<AuraTopology> topologyQueue = new LinkedList<AuraTopology>();
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = Logger.getLogger(TopologyController.class);

    private final WorkloadManagerContext context;

    private final IOManager ioManager;

    private AssemblyPipeline assemblyPipeline;

    public final UUID topologyID;



    private StateMachine.FiniteStateMachine<TopologyState, TopologyTransition> topologyFSM;

    private final TopologyContainer topologyContainer;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    /**
     * @param context
     * @param topologyID
     */
    public TopologyController(final WorkloadManagerContext context, final UUID topologyID) {
        super(true, "TopologyControllerEventDispatcher");

        // sanity check.
        if (context == null)
            throw new IllegalArgumentException("context == null");
        if (topologyID == null)
            throw new IllegalArgumentException("topologyID == null");

        this.context = context;

        this.topologyID = topologyID;

        this.topologyContainer = new TopologyContainer();

        this.ioManager = context.ioManager;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    /**
     *
     * @param topology
     */
    public void assembleTopology(final AuraTopology topology) {

        if(topologyContainer.executingTopology == null) {

            topology.topologyID = this.topologyID;

            this.topologyContainer.executingTopology = topology;

            LOG.info("ASSEMBLE TOPOLOGY '" + topologyContainer.executingTopology.name + "'");

            this.topologyFSM = createTopologyFSM();

            boolean abort = false;
            for(final Map.Entry<Pair<String,String>,Topology.Edge.TransferType> externalEdges : this.topologyContainer.executingTopology.externalEdges.entrySet()) {

                for(final AuraTopology executedTopology : topologyContainer.executedTopologies) {

                    final Topology.Node srcNode = executedTopology.nodeMap.get(externalEdges.getKey().getFirst());

                    //final Topology.Node srcNode = new Topology.Node(originalSrcNode);

                    if(srcNode != null) {

                        // Tag this node that it is already deployed.
                        srcNode.isAlreadyDeployed = true;

                        // Extend current topology with external Node.
                        topologyContainer.executingTopology.sourceMap.put(srcNode.name, srcNode);

                        topologyContainer.executingTopology.nodeMap.put(srcNode.name, srcNode);

                        final Topology.Node dstNode = topologyContainer.executingTopology.nodeMap.get(externalEdges.getKey().getSecond());

                        srcNode.addOutput(dstNode);

                        dstNode.addInput(srcNode);

                        topologyContainer.executingTopology.sourceMap.remove(dstNode.name);

                        srcNode.inputs.clear();

                        final Topology.Edge edge = new Topology.Edge(srcNode, dstNode, externalEdges.getValue(), Topology.Edge.EdgeType.FORWARD_EDGE);

                        topologyContainer.executingTopology.edges.put(externalEdges.getKey(), edge);

                        abort = true;
                        break;
                    }
                }

                if(abort)
                    break;
            }

            this.assemblyPipeline = new AssemblyPipeline(this.topologyFSM);

            assemblyPipeline.addPhase(new TopologyParallelizer());

            assemblyPipeline.addPhase(new TopologyScheduler(context.infrastructureManager));

            assemblyPipeline.addPhase(new TopologyDeployer(context.rpcManager));

            assemblyPipeline.assemble(topologyContainer.executingTopology);


            this.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_STATE_UPDATE, new IEventHandler() {

                private int finalStateCnt = 0;

                @Override
                public void handleEvent(Event e) {
                    final IOEvents.TaskControlIOEvent event = (IOEvents.TaskControlIOEvent) e;

                    final Topology.ExecutionNode en = topologyContainer.executingTopology.executionNodeMap.get(event.getTaskID());

                    // sanity check.
                    if (en == null)
                        throw new IllegalStateException();

                    en.setState((TaskStates.TaskState) event.getPayload());

                    // It is at the moment a bit clumsy to detect the processing end.
                    // We should introduce a dedicated "processing end" event...
                    if (en.getState() == TaskStates.TaskState.TASK_STATE_FINISHED ||
                            en.getState() == TaskStates.TaskState.TASK_STATE_CANCELED ||
                            en.getState() == TaskStates.TaskState.TASK_STATE_FAILURE)
                        ++finalStateCnt;

                    if (finalStateCnt == topologyContainer.executingTopology.executionNodeMap.size()) {

                        //context.workloadManager.unregisterTopology(topologyContainer.executingTopology.topologyID);
                        TopologyController.this.removeAllEventListener();
                        // Shutdown the event dispatcher threads used by this executingTopology controller
                        //shutdown();

                        topologyFSM.joinDispatcherThread();

                        finalStateCnt = 0;

                        topologyContainer.executedTopologies.add(topologyContainer.executingTopology);

                        topologyContainer.executingTopology = null;

                        if(topologyContainer.topologyQueue.size() > 0) {

                            assembleTopology(topologyContainer.topologyQueue.poll());
                        }
                    }
                }
            });

        } else {

            topologyContainer.topologyQueue.add(topology);
        }
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


        final Map<TaskStates.TaskTransition,Integer> map = new HashMap<TaskStates.TaskTransition,Integer>();

        this.addEventListener(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_TRANSITION, new IEventHandler() {

            @Override
            public void handleEvent(Event e) {
                final IOEvents.TaskControlIOEvent event = (IOEvents.TaskControlIOEvent) e;

                final TaskStates.TaskTransition transition =
                        (TaskStates.TaskTransition)((StateMachine.FSMTransitionEvent<TaskStates.TaskTransition>)event.getPayload()).getPayload();
                map.put(transition, (Integer)map.get(transition) + 1);

            }
        });







        /*final StateMachine.FSMTransitionConstraint2<TopologyState, TopologyTransition> runTransitionConstraint =
                new StateMachine.FSMTransitionConstraint2<TopologyState, TopologyTransition>(TaskStates.TaskTransition.TASK_TRANSITION_RUN) {

                    int numOfTasksToBeReady = 0;

                    @Override
                    public boolean eval(StateMachine.FSMTransitionEvent<? extends Enum<?>> event) {
                        return (++numOfTasksToBeReady) == topologyContainer.executingTopology.executionNodeMap.size();
                    }
                };*/

        final StateMachine.FSMTransitionConstraint2<TopologyState, TopologyTransition> runTransitionConstraint =
                new StateMachine.FSMTransitionConstraint2<TopologyState, TopologyTransition>(TaskStates.TaskTransition.TASK_TRANSITION_RUN) {

                    int numOfTasksToBeReady = 0;

                    final Lock threadLock = new ReentrantLock();
                    final Condition condition = threadLock.newCondition();

                    @Override
                    public boolean eval(StateMachine.FSMTransitionEvent<? extends Enum<?>> event) {

                        if (numOfTasksToBeReady == 0) {

                            final StateMachine.IFSMStateAction<TopologyState, TopologyTransition> stateHandler =
                                    new StateMachine.IFSMStateAction<TopologyState, TopologyTransition>() {
                                        @Override
                                        public void stateAction(TopologyState previousState, TopologyTransition transition, TopologyState state) {
                                            threadLock.lock();
                                            condition.signal();
                                            threadLock.unlock();
                                        }
                                    };

                            topologyFSM.addStateListener(TopologyState.TOPOLOGY_STATE_DEPLOYED, stateHandler);
                        }

                        boolean result = (++numOfTasksToBeReady) == topologyContainer.executingTopology.executionNodeMap.size();

                        if (result) {
                                threadLock.lock();
                                try {
                                    if (topologyFSM.getCurrentState() == TopologyState.TOPOLOGY_STATE_SCHEDULED) {
                                        condition.await();
                                    }
                                } catch (InterruptedException e) {
                                    // do nothing...
                                } finally {
                                    threadLock.unlock();
                                }
                                //topologyFSM.removeEventListener(TopologyState.TOPOLOGY_STATE_DEPLOYED, stateHandler);
                        }

                        return result;
                    }
                };

        final StateMachine.FSMTransitionConstraint2<TopologyState, TopologyTransition> finishTransitionConstraint =
                new StateMachine.FSMTransitionConstraint2<TopologyState, TopologyTransition>(TaskStates.TaskTransition.TASK_TRANSITION_FINISH) {

                    int numOfTasksToBeFinished = 0;

                    @Override
                    public boolean eval(StateMachine.FSMTransitionEvent<? extends Enum<?>> event) {
                        return (++numOfTasksToBeFinished) == topologyContainer.executingTopology.executionNodeMap.size();
                    }
                };


        final StateMachine.FSMTransitionConstraint2<TopologyState, TopologyTransition> cancelTransitionConstraint =
                new StateMachine.FSMTransitionConstraint2<TopologyState, TopologyTransition>(TaskStates.TaskTransition.TASK_TRANSITION_CANCEL) {

                    int numOfTasksToBeCanceled = 0;

                    @Override
                    public boolean eval(StateMachine.FSMTransitionEvent<? extends Enum<?>> event) {
                        return (++numOfTasksToBeCanceled) == topologyContainer.executingTopology.executionNodeMap.size();
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

        topologyFSM.addGlobalStateListener(new StateMachine.IFSMStateAction<TopologyState, TopologyTransition>() {

            @Override
            public void stateAction(TopologyState previousState, TopologyTransition transition, TopologyState state) {
                LOG.info("CHANGE STATE OF TOPOLOGY '" + topologyContainer.executingTopology.name + "' [" + topologyContainer.executingTopology.topologyID + "] FROM " + previousState + " TO " + state
                        + "  [" + transition.toString() + "]");
            }
        });

        topologyFSM.addStateListener(TopologyState.TOPOLOGY_STATE_RUNNING, new StateMachine.IFSMStateAction<TopologyState, TopologyTransition>() {

            @Override
            public void stateAction(TopologyState previousState, TopologyTransition transition, TopologyState state) {

                Topology.TopologyBreadthFirstTraverser.traverse(topologyContainer.executingTopology, new IVisitor<Topology.Node>() {

                    @Override
                    public void visit(final Topology.Node element) {

                        for (final Topology.ExecutionNode en : element.getExecutionNodes()) {

                            final IOEvents.TaskControlIOEvent transitionUpdate =
                                    new IOEvents.TaskControlIOEvent(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_TRANSITION);

                            transitionUpdate.setPayload(new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_RUN));
                            transitionUpdate.setTaskID(en.getNodeDescriptor().taskID);
                            transitionUpdate.setTopologyID(en.getNodeDescriptor().topologyID);

                            ioManager.sendEvent(en.getNodeDescriptor().getMachineDescriptor(), transitionUpdate);
                        }
                    }
                });
            }
        });

        topologyFSM.addStateListener(TopologyState.TOPOLOGY_STATE_FAILURE, new StateMachine.IFSMStateAction<TopologyState, TopologyTransition>() {

            @Override
            public void stateAction(TopologyState previousState, TopologyTransition transition, TopologyState state) {

                ioManager.sendEvent(topologyContainer.executingTopology.machineID, new IOEvents.ControlIOEvent(IOEvents.ControlEventType.CONTROL_EVENT_TOPOLOGY_FAILURE));

                Topology.TopologyBreadthFirstTraverser.traverse(topologyContainer.executingTopology, new IVisitor<Topology.Node>() {

                    @Override
                    public void visit(final Topology.Node element) {
                        for (final Topology.ExecutionNode en : element.getExecutionNodes()) {

                            final IOEvents.TaskControlIOEvent transitionUpdate =
                                    new IOEvents.TaskControlIOEvent(IOEvents.ControlEventType.CONTROL_EVENT_REMOTE_TASK_TRANSITION);

                            transitionUpdate.setPayload(new StateMachine.FSMTransitionEvent<>(TaskStates.TaskTransition.TASK_TRANSITION_CANCEL));
                            transitionUpdate.setTaskID(en.getNodeDescriptor().taskID);
                            transitionUpdate.setTopologyID(en.getNodeDescriptor().topologyID);

                            if (en.getState().equals(TaskStates.TaskState.TASK_STATE_RUNNING)) {
                                ioManager.sendEvent(en.getNodeDescriptor().getMachineDescriptor(), transitionUpdate);
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
                event.setPayload(TopologyController.this.topologyContainer.executingTopology.name);

                ioManager.sendEvent(topologyContainer.executingTopology.machineID, event);

                // Shutdown the event dispatcher threads used by this executingTopology controller
                TopologyController.this.topologyFSM.shutdown();
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
