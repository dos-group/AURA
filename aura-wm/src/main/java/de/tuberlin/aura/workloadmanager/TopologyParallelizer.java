package de.tuberlin.aura.workloadmanager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.common.utils.PipelineAssembler.AssemblyPhase;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskBindingDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDescriptor;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.Edge;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.ExecutionNode;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.Node;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.TopologyBreadthFirstTraverser;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.Visitor;
import de.tuberlin.aura.core.task.usercode.UserCode;
import de.tuberlin.aura.workloadmanager.TopologyEvents.TopologyStateTransitionEvent;
import de.tuberlin.aura.workloadmanager.TopologyStateMachine.TopologyTransition;

public class TopologyParallelizer extends AssemblyPhase<AuraTopology, AuraTopology> {

	// ---------------------------------------------------
	// Public.
	// ---------------------------------------------------

	@Override
	public AuraTopology apply(AuraTopology topology) {

		parallelizeTopology(topology);

		dispatcher.dispatchEvent(new TopologyStateTransitionEvent(TopologyTransition.TOPOLOGY_TRANSITION_PARALLELIZE));

		return topology;
	}

	// ---------------------------------------------------
	// Private.
	// ---------------------------------------------------

	private void parallelizeTopology(final AuraTopology topology) {
		// sanity check.
		if (topology == null)
			throw new IllegalArgumentException("topology == null");

		final Map<UUID, ExecutionNode> executionNodeMap = new HashMap<UUID, ExecutionNode>();

		// First pass, create task descriptors.
		TopologyBreadthFirstTraverser.traverse(topology, new Visitor<Node>() {

			@Override
			public void visit(final Node element) {
				final UserCode userCode = topology.userCodeMap.get(element.name);
				for (int index = 0; index < element.degreeOfParallelism; ++index) {
					final UUID taskID = UUID.randomUUID();
					final TaskDescriptor taskDescriptor = new TaskDescriptor(topology.topologyID, taskID, element.name,
						userCode);
					final UUID executionNodeID = UUID.randomUUID();
					final ExecutionNode executionNode = new ExecutionNode(executionNodeID, element);
					executionNode.setTaskDescriptor(taskDescriptor);
					element.addExecutionNode(executionNode);
					executionNodeMap.put(taskID, executionNode);
				}
			}
		});

		topology.setExecutionNodes(executionNodeMap);

		// Second pass, create binding descriptors.
		TopologyBreadthFirstTraverser.traverse(topology, new Visitor<Node>() {

			@Override
			public void visit(final Node element) {

				// TODO: reduce code!

				// Bind inputs of the execution nodes.
				final Map<UUID, Map<UUID, List<TaskDescriptor>>> gateExecutionNodeInputs = new HashMap<UUID, Map<UUID, List<TaskDescriptor>>>();
				for (final Node n : element.getInputs()) {

					final Edge ie = topology.edges.get(new Pair<String, String>(n.name, element.name));
					switch (ie.transferType) {

					case ALL_TO_ALL: {

						for (final ExecutionNode dstEN : element.getExecutionNodes()) {

							Map<UUID, List<TaskDescriptor>> executionNodeInputs = gateExecutionNodeInputs
								.get(dstEN.uid);
							if (executionNodeInputs == null) {
								executionNodeInputs = new HashMap<UUID, List<TaskDescriptor>>();
								gateExecutionNodeInputs.put(dstEN.uid, executionNodeInputs);
							}

							List<TaskDescriptor> inputDescriptors = executionNodeInputs.get(n.uid);
							if (inputDescriptors == null) {
								inputDescriptors = new ArrayList<TaskDescriptor>();
								executionNodeInputs.put(n.uid, inputDescriptors);
							}

							for (final ExecutionNode srcEN : n.getExecutionNodes())
								inputDescriptors.add(srcEN.getTaskDescriptor());
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

								final int numOfLinks = numOfSrcLinks +
									(index++ < numOfNodesWithOneAdditionalLink ? 1 : 0);

								int i = 0;
								while (i++ < numOfLinks) {
									final ExecutionNode dstEN = dstIter.next();

									Map<UUID, List<TaskDescriptor>> executionNodeInputs = gateExecutionNodeInputs
										.get(dstEN.uid);
									if (executionNodeInputs == null) {
										executionNodeInputs = new HashMap<UUID, List<TaskDescriptor>>();
										gateExecutionNodeInputs.put(dstEN.uid, executionNodeInputs);
									}

									List<TaskDescriptor> inputDescriptors = executionNodeInputs.get(n.uid);
									if (inputDescriptors == null) {
										inputDescriptors = new ArrayList<TaskDescriptor>();
										executionNodeInputs.put(n.uid, inputDescriptors);
									}

									inputDescriptors.add(srcEN.getTaskDescriptor());
								}
							}

						} else { // dstDegree < srcDegree

							final int numOfDstLinks = srcDegree / dstDegree; // number of links per dst execution node.
							final int numOfNodesWithOneAdditionalLink = srcDegree % dstDegree;
							final Iterator<ExecutionNode> srcIter = n.getExecutionNodes().iterator();

							int index = 0;
							for (final ExecutionNode dstEN : element.getExecutionNodes()) {

								Map<UUID, List<TaskDescriptor>> executionNodeInputs = gateExecutionNodeInputs
									.get(dstEN.uid);
								if (executionNodeInputs == null) {
									executionNodeInputs = new HashMap<UUID, List<TaskDescriptor>>();
									gateExecutionNodeInputs.put(dstEN.uid, executionNodeInputs);
								}

								List<TaskDescriptor> inputDescriptors = executionNodeInputs.get(n.uid);
								if (inputDescriptors == null) {
									inputDescriptors = new ArrayList<TaskDescriptor>();
									executionNodeInputs.put(n.uid, inputDescriptors);
								}

								final int numOfLinks = numOfDstLinks +
									(index++ < numOfNodesWithOneAdditionalLink ? 1 : 0);

								int i = 0;
								while (i++ < numOfLinks) {
									final ExecutionNode srcEN = srcIter.next();
									inputDescriptors.add(srcEN.getTaskDescriptor());
								}
							}
						}

					}
						break;
					}
				}

				// Bind outputs of the execution nodes.
				final Map<UUID, Map<UUID, List<TaskDescriptor>>> gateExecutionNodeOutputs = new HashMap<UUID, Map<UUID, List<TaskDescriptor>>>();
				for (final Node n : element.getOutputs()) {

					final Edge ie = topology.edges.get(new Pair<String, String>(element.name, n.name));
					switch (ie.transferType) {

					case ALL_TO_ALL: {

						for (final ExecutionNode srcEN : element.getExecutionNodes()) {

							Map<UUID, List<TaskDescriptor>> executionNodeOutputs = gateExecutionNodeOutputs
								.get(srcEN.uid);
							if (executionNodeOutputs == null) {
								executionNodeOutputs = new HashMap<UUID, List<TaskDescriptor>>();
								gateExecutionNodeOutputs.put(srcEN.uid, executionNodeOutputs);
							}

							List<TaskDescriptor> outputDescriptors = executionNodeOutputs.get(n.uid);
							if (outputDescriptors == null) {
								outputDescriptors = new ArrayList<TaskDescriptor>();
								executionNodeOutputs.put(n.uid, outputDescriptors);
							}

							for (final ExecutionNode dstEN : n.getExecutionNodes())
								outputDescriptors.add(dstEN.getTaskDescriptor());
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

								Map<UUID, List<TaskDescriptor>> executionNodeOutputs = gateExecutionNodeOutputs
									.get(srcEN.uid);
								if (executionNodeOutputs == null) {
									executionNodeOutputs = new HashMap<UUID, List<TaskDescriptor>>();
									gateExecutionNodeOutputs.put(srcEN.uid, executionNodeOutputs);
								}

								List<TaskDescriptor> outputDescriptors = executionNodeOutputs.get(n.uid);
								if (outputDescriptors == null) {
									outputDescriptors = new ArrayList<TaskDescriptor>();
									executionNodeOutputs.put(n.uid, outputDescriptors);
								}

								final int numOfLinks = numOfSrcLinks +
									(index++ < numOfNodesWithOneAdditionalLink ? 1 : 0);

								int i = 0;
								while (i++ < numOfLinks) {
									final ExecutionNode dstEN = dstIter.next();
									outputDescriptors.add(dstEN.getTaskDescriptor());
								}
							}

						} else { // dstDegree < srcDegree

							final int numOfDstLinks = srcDegree / dstDegree; // number of links per dst execution node.
							final int numOfNodesWithOneAdditionalLink = srcDegree % dstDegree;
							final Iterator<ExecutionNode> srcIter = element.getExecutionNodes().iterator();

							int index = 0;
							for (final ExecutionNode dstEN : n.getExecutionNodes()) {

								final int numOfLinks = numOfDstLinks +
									(index++ < numOfNodesWithOneAdditionalLink ? 1 : 0);

								int i = 0;
								while (i++ < numOfLinks) {
									final ExecutionNode srcEN = srcIter.next();

									Map<UUID, List<TaskDescriptor>> executionNodeOutputs = gateExecutionNodeOutputs
										.get(srcEN.uid);
									if (executionNodeOutputs == null) {
										executionNodeOutputs = new HashMap<UUID, List<TaskDescriptor>>();
										gateExecutionNodeOutputs.put(srcEN.uid, executionNodeOutputs);
									}

									List<TaskDescriptor> outputDescriptors = executionNodeOutputs.get(n.uid);
									if (outputDescriptors == null) {
										outputDescriptors = new ArrayList<TaskDescriptor>();
										executionNodeOutputs.put(n.uid, outputDescriptors);
									}

									outputDescriptors.add(dstEN.getTaskDescriptor());
								}
							}
						}

					}
						break;
					}
				}

				// Assign the binding descriptors to the execution nodes.
				for (final ExecutionNode en : element.getExecutionNodes()) {

					final Map<UUID, List<TaskDescriptor>> inputsPerGate = gateExecutionNodeInputs.get(en.uid);

					List<List<TaskDescriptor>> inputsPerGateList = null;
					if (inputsPerGate != null) {
						final Collection<List<TaskDescriptor>> inputsPerGateCollection = inputsPerGate.values();
						if (inputsPerGateCollection instanceof List)
							inputsPerGateList = (List<List<TaskDescriptor>>) inputsPerGateCollection;
						else
							inputsPerGateList = new ArrayList<List<TaskDescriptor>>(inputsPerGateCollection);
					}

					final Map<UUID, List<TaskDescriptor>> outputsPerGate = gateExecutionNodeOutputs.get(en.uid);

					List<List<TaskDescriptor>> outputsPerGateList = null;
					if (outputsPerGate != null) {
						final Collection<List<TaskDescriptor>> outputsPerGateCollection = outputsPerGate.values();
						if (outputsPerGateCollection instanceof List)
							outputsPerGateList = (List<List<TaskDescriptor>>) outputsPerGate.values();
						else
							outputsPerGateList = new ArrayList<List<TaskDescriptor>>(outputsPerGate.values());
					}

					final TaskBindingDescriptor bindingDescriptor =
						new TaskBindingDescriptor(en.getTaskDescriptor(),
							inputsPerGateList != null ? inputsPerGateList : new ArrayList<List<TaskDescriptor>>(),
							outputsPerGateList != null ? outputsPerGateList : new ArrayList<List<TaskDescriptor>>());

					en.setTaskBindingDescriptor(bindingDescriptor);
				}
			}
		});
	}
}
