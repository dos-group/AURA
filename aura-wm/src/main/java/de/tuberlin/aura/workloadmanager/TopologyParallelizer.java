package de.tuberlin.aura.workloadmanager;

import java.util.*;

import de.tuberlin.aura.core.common.statemachine.StateMachine;
import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.common.utils.PipelineAssembler.AssemblyPhase;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.descriptors.Descriptors.AbstractNodeDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.NodeBindingDescriptor;
import de.tuberlin.aura.core.task.common.TaskStates;
import de.tuberlin.aura.core.task.usercode.UserCode;
import de.tuberlin.aura.core.topology.Topology.*;
import de.tuberlin.aura.core.topology.TopologyStates.TopologyTransition;

public class TopologyParallelizer extends AssemblyPhase<AuraTopology, AuraTopology> {

    // ---------------------------------------------------
    // Public.
    // ---------------------------------------------------

    @Override
    public AuraTopology apply(AuraTopology topology) {

        parallelizeTopology(topology);

        dispatcher.dispatchEvent(new StateMachine.FSMTransitionEvent<>(TopologyTransition.TOPOLOGY_TRANSITION_PARALLELIZE));

        return topology;
    }

    // ---------------------------------------------------
    // Private.
    // ---------------------------------------------------

    private void parallelizeTopology(final AuraTopology topology) {
        // sanity check.
        if (topology == null)
            throw new IllegalArgumentException("topology == null");

        final Map<UUID, ExecutionNode> executionNodeMap = new HashMap<>();

        // First pass, create task descriptors.
        TopologyBreadthFirstTraverser.traverse(topology, new IVisitor<Node>() {

            @Override
            public void visit(final Node element) {

                if(element.isAlreadyDeployed) {
                    for(final ExecutionNode en : element.getExecutionNodes()) {
                        // Reset the task state.
                        en.setState(TaskStates.TaskState.TASK_STATE_CREATED);
                        executionNodeMap.put(en.getNodeDescriptor().taskID, en);
                    }
                    return;
                }

                final List<UserCode> userCodeList = topology.userCodeMap.get(element.name);
                for (int index = 0; index < element.degreeOfParallelism; ++index) {
                    final UUID taskID = UUID.randomUUID();

                    final Descriptors.AbstractNodeDescriptor nodeDescriptor;

                    if (element instanceof ComputationNode)
                        nodeDescriptor = new Descriptors.ComputationNodeDescriptor(topology.topologyID, taskID, index, element.name, userCodeList);
                    else if (element instanceof OperatorNode)
                        nodeDescriptor = new Descriptors.OperatorNodeDescriptor(
                                topology.topologyID,
                                taskID,
                                index,
                                element.name,
                                userCodeList,
                                ((OperatorNode)element).properties
                        );
                    else if (element instanceof StorageNode)
                        nodeDescriptor = new Descriptors.StorageNodeDescriptor(topology.topologyID, taskID, index, element.name);
                    else if (element instanceof Node)
                        nodeDescriptor = new Descriptors.AbstractNodeDescriptor(topology.topologyID, taskID, index, element.name, userCodeList);
                    else
                        throw new IllegalStateException();

                    final UUID executionNodeID = UUID.randomUUID();
                    final ExecutionNode executionNode = new ExecutionNode(executionNodeID, index, element);
                    executionNode.setNodeDescriptor(nodeDescriptor);
                    element.addExecutionNode(executionNode);
                    executionNodeMap.put(taskID, executionNode);
                }
            }
        });

        topology.setExecutionNodes(executionNodeMap);

        // Second pass, create binding descriptors.
        TopologyBreadthFirstTraverser.traverse(topology, new IVisitor<Node>() {

            @Override
            public void visit(final Node element) {

                // TODO: reduce code!

                // Bind inputs of the execution nodes.
                final Map<UUID, Map<UUID, List<Descriptors.AbstractNodeDescriptor>>> gateExecutionNodeInputs = new HashMap<>();
                for (final Node n : element.getInputs()) {

                    if(element.isAlreadyDeployed)
                        continue;

                    final Edge ie = topology.edges.get(new Pair<>(n.name, element.name));

                    switch (ie.transferType) {

                        case ALL_TO_ALL: {

                            for (final ExecutionNode dstEN : element.getExecutionNodes()) {

                                Map<UUID, List<Descriptors.AbstractNodeDescriptor>> executionNodeInputs = gateExecutionNodeInputs.get(dstEN.uid);
                                if (executionNodeInputs == null) {
                                    executionNodeInputs = new LinkedHashMap<>();
                                    gateExecutionNodeInputs.put(dstEN.uid, executionNodeInputs);
                                }

                                List<Descriptors.AbstractNodeDescriptor> inputDescriptors = executionNodeInputs.get(n.uid);
                                if (inputDescriptors == null) {
                                    inputDescriptors = new ArrayList<>();
                                    executionNodeInputs.put(n.uid, inputDescriptors);
                                }

                                // Assignment of channels
                                for (final ExecutionNode srcEN : n.getExecutionNodes()) {
                                    inputDescriptors.add(srcEN.getNodeDescriptor());
                                }
                            }
                        }
                        break;

                        case POINT_TO_POINT: {

                            final int dstDegree = element.degreeOfParallelism;
                            final int srcDegree = n.degreeOfParallelism;

                            if (dstDegree >= srcDegree) {

                                final int numOfSrcLinks = dstDegree / srcDegree;
                                final int numOfNodesWithOneAdditionalLink = dstDegree % srcDegree;
                                final Iterator<ExecutionNode> dstIter = element.getExecutionNodes().iterator();

                                int index = 0;
                                for (final ExecutionNode srcEN : n.getExecutionNodes()) {

                                    final int numOfLinks = numOfSrcLinks + (index++ < numOfNodesWithOneAdditionalLink ? 1 : 0);

                                    int i = 0;
                                    while (i++ < numOfLinks) {
                                        final ExecutionNode dstEN = dstIter.next();

                                        Map<UUID, List<Descriptors.AbstractNodeDescriptor>> executionNodeInputs = gateExecutionNodeInputs.get(dstEN.uid);
                                        if (executionNodeInputs == null) {
                                            executionNodeInputs = new LinkedHashMap<>();
                                            gateExecutionNodeInputs.put(dstEN.uid, executionNodeInputs);
                                        }

                                        List<Descriptors.AbstractNodeDescriptor> inputDescriptors = executionNodeInputs.get(n.uid);
                                        if (inputDescriptors == null) {
                                            inputDescriptors = new ArrayList<>();
                                            executionNodeInputs.put(n.uid, inputDescriptors);
                                        }

                                        inputDescriptors.add(srcEN.getNodeDescriptor());
                                    }
                                }

                            } else { // dstDegree < srcDegree

                                final int numOfDstLinks = srcDegree / dstDegree; // number of links
                                // per dst
                                // execution node.
                                final int numOfNodesWithOneAdditionalLink = srcDegree % dstDegree;
                                final Iterator<ExecutionNode> srcIter = n.getExecutionNodes().iterator();

                                int index = 0;
                                for (final ExecutionNode dstEN : element.getExecutionNodes()) {

                                    Map<UUID, List<Descriptors.AbstractNodeDescriptor>> executionNodeInputs = gateExecutionNodeInputs.get(dstEN.uid);
                                    if (executionNodeInputs == null) {
                                        executionNodeInputs = new LinkedHashMap<>();
                                        gateExecutionNodeInputs.put(dstEN.uid, executionNodeInputs);
                                    }

                                    List<Descriptors.AbstractNodeDescriptor> inputDescriptors = executionNodeInputs.get(n.uid);
                                    if (inputDescriptors == null) {
                                        inputDescriptors = new ArrayList<>();
                                        executionNodeInputs.put(n.uid, inputDescriptors);
                                    }

                                    final int numOfLinks = numOfDstLinks + (index++ < numOfNodesWithOneAdditionalLink ? 1 : 0);

                                    int i = 0;
                                    while (i++ < numOfLinks) {
                                        final ExecutionNode srcEN = srcIter.next();
                                        inputDescriptors.add(srcEN.getNodeDescriptor());
                                    }
                                }
                            }

                        }
                        break;
                    }
                }

                // Bind outputs of the execution nodes.
                final Map<UUID, Map<UUID, List<Descriptors.AbstractNodeDescriptor>>> gateExecutionNodeOutputs = new HashMap<>();
                for (final Node n : element.getOutputs()) {

                    final Edge ie = topology.edges.get(new Pair<>(element.name, n.name));

                    switch (ie.transferType) {

                        case ALL_TO_ALL: {

                            for (final ExecutionNode srcEN : element.getExecutionNodes()) {

                                Map<UUID, List<Descriptors.AbstractNodeDescriptor>> executionNodeOutputs = gateExecutionNodeOutputs.get(srcEN.uid);
                                if (executionNodeOutputs == null) {
                                    executionNodeOutputs = new LinkedHashMap<>();
                                    gateExecutionNodeOutputs.put(srcEN.uid, executionNodeOutputs);
                                }

                                List<Descriptors.AbstractNodeDescriptor> outputDescriptors = executionNodeOutputs.get(n.uid);
                                if (outputDescriptors == null) {
                                    outputDescriptors = new ArrayList<>();
                                    executionNodeOutputs.put(n.uid, outputDescriptors);
                                }

                                for (final ExecutionNode dstEN : n.getExecutionNodes())
                                    outputDescriptors.add(dstEN.getNodeDescriptor());
                            }

                        }
                        break;

                        case POINT_TO_POINT: {

                            final int dstDegree = n.degreeOfParallelism;
                            final int srcDegree = element.degreeOfParallelism;

                            if (dstDegree >= srcDegree) {

                                final int numOfSrcLinks = dstDegree / srcDegree;
                                final int numOfNodesWithOneAdditionalLink = dstDegree % srcDegree;
                                final Iterator<ExecutionNode> dstIter = n.getExecutionNodes().iterator();

                                int index = 0;
                                for (final ExecutionNode srcEN : element.getExecutionNodes()) {

                                    Map<UUID, List<Descriptors.AbstractNodeDescriptor>> executionNodeOutputs = gateExecutionNodeOutputs.get(srcEN.uid);
                                    if (executionNodeOutputs == null) {
                                        executionNodeOutputs = new LinkedHashMap<>();
                                        gateExecutionNodeOutputs.put(srcEN.uid, executionNodeOutputs);
                                    }

                                    List<Descriptors.AbstractNodeDescriptor> outputDescriptors = executionNodeOutputs.get(n.uid);
                                    if (outputDescriptors == null) {
                                        outputDescriptors = new ArrayList<>();
                                        executionNodeOutputs.put(n.uid, outputDescriptors);
                                    }

                                    final int numOfLinks = numOfSrcLinks + (index++ < numOfNodesWithOneAdditionalLink ? 1 : 0);

                                    int i = 0;
                                    while (i++ < numOfLinks) {
                                        final ExecutionNode dstEN = dstIter.next();
                                        outputDescriptors.add(dstEN.getNodeDescriptor());
                                    }
                                }

                            } else { // dstDegree < srcDegree

                                final int numOfDstLinks = srcDegree / dstDegree; // number of links
                                // per dst
                                // execution node.
                                final int numOfNodesWithOneAdditionalLink = srcDegree % dstDegree;
                                final Iterator<ExecutionNode> srcIter = element.getExecutionNodes().iterator();

                                int index = 0;
                                for (final ExecutionNode dstEN : n.getExecutionNodes()) {

                                    final int numOfLinks = numOfDstLinks + (index++ < numOfNodesWithOneAdditionalLink ? 1 : 0);

                                    int i = 0;
                                    while (i++ < numOfLinks) {
                                        final ExecutionNode srcEN = srcIter.next();

                                        Map<UUID, List<Descriptors.AbstractNodeDescriptor>> executionNodeOutputs = gateExecutionNodeOutputs.get(srcEN.uid);
                                        if (executionNodeOutputs == null) {
                                            executionNodeOutputs = new LinkedHashMap<>();
                                            gateExecutionNodeOutputs.put(srcEN.uid, executionNodeOutputs);
                                        }

                                        List<AbstractNodeDescriptor> outputDescriptors = executionNodeOutputs.get(n.uid);
                                        if (outputDescriptors == null) {
                                            outputDescriptors = new ArrayList<>();
                                            executionNodeOutputs.put(n.uid, outputDescriptors);
                                        }

                                        outputDescriptors.add(dstEN.getNodeDescriptor());
                                    }
                                }
                            }

                        }
                        break;
                    }
                }

                // Assign the binding descriptors to the execution nodes.
                for (final ExecutionNode en : element.getExecutionNodes()) {

                    final Map<UUID, List<Descriptors.AbstractNodeDescriptor>> inputsPerGate = gateExecutionNodeInputs.get(en.uid);

                    List<List<Descriptors.AbstractNodeDescriptor>> inputsPerGateList = null;

                    if (inputsPerGate != null) {
                        final Collection<List<Descriptors.AbstractNodeDescriptor>> inputsPerGateCollection = inputsPerGate.values(); // TODO: BUG!
                        if (inputsPerGateCollection instanceof List) {
                            inputsPerGateList = (List<List<Descriptors.AbstractNodeDescriptor>>) inputsPerGateCollection;
                        } else
                            inputsPerGateList = new ArrayList<>(inputsPerGateCollection);
                    }

                    final Map<UUID, List<Descriptors.AbstractNodeDescriptor>> outputsPerGate = gateExecutionNodeOutputs.get(en.uid);

                    List<List<Descriptors.AbstractNodeDescriptor>> outputsPerGateList = null;
                    if (outputsPerGate != null) {
                        final Collection<List<Descriptors.AbstractNodeDescriptor>> outputsPerGateCollection = outputsPerGate.values();
                        if (outputsPerGateCollection instanceof List)
                            outputsPerGateList = (List<List<Descriptors.AbstractNodeDescriptor>>) outputsPerGate.values();
                        else
                            outputsPerGateList = new ArrayList<>(outputsPerGate.values());
                    }

                    final Descriptors.NodeBindingDescriptor bindingDescriptor =
                            new NodeBindingDescriptor(en.getNodeDescriptor(), inputsPerGateList != null
                                    ? inputsPerGateList
                                    : new ArrayList<List<Descriptors.AbstractNodeDescriptor>>(), outputsPerGateList != null
                                    ? outputsPerGateList
                                    : new ArrayList<List<Descriptors.AbstractNodeDescriptor>>());

                    en.setNodeBindingDescriptor(bindingDescriptor);
                }
            }
        });
    }
}
