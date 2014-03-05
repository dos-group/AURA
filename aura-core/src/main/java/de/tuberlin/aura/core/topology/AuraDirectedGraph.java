package de.tuberlin.aura.core.topology;

import java.io.Serializable;
import java.util.*;

import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskBindingDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDescriptor;
import de.tuberlin.aura.core.topology.AuraDirectedGraph.AuraTopology.DeploymentType;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskState;
import de.tuberlin.aura.core.task.usercode.UserCode;
import de.tuberlin.aura.core.task.usercode.UserCodeExtractor;

public class AuraDirectedGraph {

	// Disallow instantiation.
	private AuraDirectedGraph() {
	}

	/**
     *
     */
	public static final class AuraTopology implements Serializable {

		private static final long serialVersionUID = 1;

		// ---------------------------------------------------
		// Aura Topology Properties.
		// ---------------------------------------------------

        public static enum MonitoringType {

            NO_MONITORING,

            TASK_MONITORING,

            TOPOLOGY_MONITORING
        }

		public static enum DeploymentType {

			LAZY,

			EAGER
		}

		// ---------------------------------------------------
		// Constructor.
		// ---------------------------------------------------

		public AuraTopology(final UUID machineID,
                            final String name,
				            final UUID topologyID,
				            final Map<String, Node> nodeMap,
				            final Map<String, Node> sourceMap,
				            final Map<String, Node> sinkMap,
				            final Map<Pair<String, String>, Edge> edges,
				            final Map<String, UserCode> userCodeMap,
				            final Map<UUID, Node> uidNodeMap,
				            final DeploymentType deploymentType,
                            final EnumSet<MonitoringType> monitoringProperties) {
			// sanity check.
            if (machineID == null)
                throw new IllegalArgumentException("machineID == null");
			if (name == null)
				throw new IllegalArgumentException("name == null");
			if (topologyID == null)
				throw new IllegalArgumentException("topologyID == null");
			if (nodeMap == null)
				throw new IllegalArgumentException("nodeMap == null");
			if (sourceMap == null)
				throw new IllegalArgumentException("sourceMap == null");
			if (sinkMap == null)
				throw new IllegalArgumentException("sinkMap == null");
			if (edges == null)
				throw new IllegalArgumentException("edges == null");
			if (userCodeMap == null)
				throw new IllegalArgumentException("userCodeMap == null");
			if (uidNodeMap == null)
				throw new IllegalArgumentException("uidNodeMap == null");
			if (deploymentType == null)
				throw new IllegalArgumentException("deploymentType == null");
            if (monitoringProperties == null)
                throw new IllegalArgumentException("monitoringType == null");

            this.machineID = machineID;

			this.name = name;

			this.topologyID = topologyID;

			this.nodeMap = Collections.unmodifiableMap(nodeMap);

			this.sourceMap = Collections.unmodifiableMap(sourceMap);

			this.sinkMap = Collections.unmodifiableMap(sinkMap);

			this.edges = Collections.unmodifiableMap(edges);

			this.userCodeMap = Collections.unmodifiableMap(userCodeMap);

			this.uidNodeMap = Collections.unmodifiableMap(uidNodeMap);

			this.deploymentType = deploymentType;

            this.monitoringProperties = EnumSet.copyOf(monitoringProperties);

			this.executionNodeMap = null;
		}

		// ---------------------------------------------------
		// Fields.
		// ---------------------------------------------------

        public final UUID machineID;

		public final String name;

		public final UUID topologyID;

		public final Map<String, Node> nodeMap;

		public final Map<String, Node> sourceMap;

		public final Map<String, Node> sinkMap;

		public final Map<Pair<String, String>, Edge> edges;

		public final Map<String, UserCode> userCodeMap;

		public final Map<UUID, Node> uidNodeMap;

		public final DeploymentType deploymentType;

        public final EnumSet<MonitoringType> monitoringProperties;

        public Map<UUID, ExecutionNode> executionNodeMap;

        // ---------------------------------------------------
		// Public.
		// ---------------------------------------------------

		public void setExecutionNodes(final Map<UUID, ExecutionNode> executionNodeMap) {
			// sanity check.
			if (executionNodeMap == null)
				throw new IllegalArgumentException("executionNodes == null");
			if (this.executionNodeMap != null)
				throw new IllegalStateException("execution nodes already set");

			this.executionNodeMap = Collections.unmodifiableMap(executionNodeMap);
		}
	}

	/**
     *
     */
	public static final class AuraTopologyBuilder {

		// ---------------------------------------------------
		// Inner Classes.
		// ---------------------------------------------------

		public final class NodeConnector {

			protected NodeConnector(final AuraTopologyBuilder tb) {
				this.tb = tb;
				this.edges = new ArrayList<Pair<String, String>>();
				this.edgeProperties = new HashMap<Pair<String, String>, List<Object>>();
			}

			private final AuraTopologyBuilder tb;

			private final List<Pair<String, String>> edges;

			private final Map<Pair<String, String>, List<Object>> edgeProperties;

			private Node srcNode;

			public NodeConnector currentSource(final Node srcNode) {
				this.srcNode = srcNode;
				return this;
			}

			public AuraTopologyBuilder connectTo(final String dstNodeName,
					final Edge.TransferType transferType,
					final Edge.EdgeType edgeType,
					final Node.DataPersistenceType dataLifeTime,
					final Node.ExecutionType executionType) {
				// sanity check.
				if (dstNodeName == null)
					throw new IllegalArgumentException("dstNode == null");
				if (transferType == null)
					throw new IllegalArgumentException("transferType == null");
				if (edgeType == null)
					throw new IllegalArgumentException("edgeType == null");
				if (dataLifeTime == null)
					throw new IllegalArgumentException("dataLifeTime == null");
				if (executionType == null)
					throw new IllegalArgumentException("executionType == null");

				Object[] properties = { transferType, edgeType, dataLifeTime, executionType };
				edges.add(new Pair<String, String>(srcNode.name, dstNodeName));
				edgeProperties.put(new Pair<String, String>(srcNode.name, dstNodeName), Arrays.asList(properties));
				return tb;
			}

			public AuraTopologyBuilder connectTo(final String dstNodeName, final Edge.TransferType transferType) {
				return connectTo(dstNodeName, transferType, Edge.EdgeType.FORWARD_EDGE,
					Node.DataPersistenceType.EPHEMERAL, Node.ExecutionType.PIPELINED);
			}

			public AuraTopologyBuilder connectTo(final String dstNodeName, final Edge.TransferType transferType,
					final Edge.EdgeType edgeType) {
				return connectTo(dstNodeName, transferType, edgeType, Node.DataPersistenceType.EPHEMERAL,
					Node.ExecutionType.PIPELINED);
			}

			public AuraTopologyBuilder connectTo(final String dstNodeName, final Edge.TransferType transferType,
					final Edge.EdgeType edgeType, final Node.DataPersistenceType dataLifeTime) {
				return connectTo(dstNodeName, transferType, edgeType, dataLifeTime, Node.ExecutionType.PIPELINED);
			}

			public List<Pair<String, String>> getEdges() {
				return Collections.unmodifiableList(edges);
			}

			public Map<Pair<String, String>, List<Object>> getEdgeProperties() {
				return Collections.unmodifiableMap(edgeProperties);
			}
		}

		// ---------------------------------------------------
		// Constructor.
		// ---------------------------------------------------

		public AuraTopologyBuilder(final UUID machineID, final UserCodeExtractor codeExtractor) {
			// sanity check.
            if (machineID == null)
                throw new IllegalArgumentException("machineID == null");
			if (codeExtractor == null)
				throw new IllegalArgumentException("codeExtractor == null");

            this.machineID = machineID;

			this.nodeMap = new HashMap<String, Node>();

			this.sourceMap = new HashMap<String, Node>();

			this.sinkMap = new HashMap<String, Node>();

			this.edges = new HashMap<Pair<String, String>, Edge>();

			this.nodeConnector = new NodeConnector(this);

			this.codeExtractor = codeExtractor;

			this.userCodeMap = new HashMap<String, UserCode>();

            this.userCodeClazzMap = new HashMap<String, Class<?>>();

			this.uidNodeMap = new HashMap<UUID, Node>();
		}

		// ---------------------------------------------------
		// Fields.
		// ---------------------------------------------------

        private final UUID machineID;

		private final Map<String, Node> nodeMap;

		private final Map<String, Node> sourceMap;

		private final Map<String, Node> sinkMap;

		private final Map<Pair<String, String>, Edge> edges;

		private final NodeConnector nodeConnector;

		private final UserCodeExtractor codeExtractor;

		private final Map<String, UserCode> userCodeMap;

        private final Map<String, Class<?>> userCodeClazzMap;

		private final Map<UUID, Node> uidNodeMap;

		private boolean isBuilded = false;

		// ---------------------------------------------------
		// Public.
		// ---------------------------------------------------

		public NodeConnector addNode(final Node node, Class<?> userCodeClazz) {
			// sanity check.
			if (node == null)
				throw new IllegalArgumentException("node == null");
            if(userCodeClazz == null)
                throw new IllegalArgumentException("userCodeClazz == null");

			if (nodeMap.containsKey(node.name))
				throw new IllegalStateException("node already exists");

			nodeMap.put(node.name, node);
			sourceMap.put(node.name, node);
			sinkMap.put(node.name, node);
			uidNodeMap.put(node.uid, node);
            userCodeClazzMap.put(node.name, userCodeClazz);

			return nodeConnector.currentSource(node);
		}

		public NodeConnector and() {
			return nodeConnector;
		}

		public AuraTopology build(final String name) {
			return build(name, EnumSet.of(AuraTopology.MonitoringType.NO_MONITORING), DeploymentType.EAGER);
		}

        public AuraTopology build(final String name,
                                  final EnumSet<AuraTopology.MonitoringType> monitoringProperties) {
            return build(name, monitoringProperties, DeploymentType.EAGER);
        }

        public AuraTopology build(final String name,
                                  final EnumSet<AuraTopology.MonitoringType> monitoringProperties,
                                  final DeploymentType deploymentType) {
			// sanity check.
			if (name == null)
				throw new IllegalArgumentException("name == null");
            if (monitoringProperties == null)
                throw new IllegalArgumentException("monitoringProperties == null");
			if (deploymentType == null)
				throw new IllegalArgumentException("deploymentType == null");
            // sanity check of monitoring properties.
            if( monitoringProperties.contains(AuraTopology.MonitoringType.NO_MONITORING)
                    && monitoringProperties.size() > 1 )
                throw new IllegalStateException();

            if (!isBuilded) {

				final Map<Pair<String, String>, List<Object>> edgeProperties = nodeConnector.getEdgeProperties();

				for (final Pair<String, String> entry : nodeConnector.getEdges()) {
					final Node srcNode = nodeMap.get(entry.getFirst());
					final Node dstNode = nodeMap.get(entry.getSecond());
					srcNode.addOutput(dstNode);
					dstNode.addInput(srcNode);
				}

				for (final Pair<String, String> entry : nodeConnector.getEdges()) {

					final Node srcNode = nodeMap.get(entry.getFirst());
					final Node dstNode = nodeMap.get(entry.getSecond());
					final List<Object> properties = edgeProperties.get(new Pair<String, String>(srcNode.name,
						dstNode.name));
					final Edge.TransferType transferType = (Edge.TransferType) properties.get(0);
					final Edge.EdgeType edgeType = (Edge.EdgeType) properties.get(1);

					if (edgeType == Edge.EdgeType.BACKWARD_EDGE) {
						if (!validateBackCouplingEdge(new HashSet<Node>(), srcNode, dstNode))
							throw new IllegalStateException(srcNode.name + " to " + dstNode.name
								+ "is not a back coupling edge");
					}

					edges.put(new Pair<String, String>(srcNode.name, dstNode.name),
						new Edge(srcNode, dstNode, transferType, edgeType));

					if (edgeType != Edge.EdgeType.BACKWARD_EDGE) {
						sourceMap.remove(dstNode.name);
						sinkMap.remove(srcNode.name);
					}
				}

				for (final Node n : nodeMap.values()) {
                    final Class<?> userCodeClazz = userCodeClazzMap.get(n.name);
                    final UserCode uc = codeExtractor.extractUserCodeClass(userCodeClazz);
					userCodeMap.put(n.name, uc);
				}

				isBuilded = true;
			}

			// Every call to build gives us the same topology with a new id.
			final UUID topologyID = UUID.randomUUID();

			return new AuraTopology(machineID, name, topologyID, nodeMap, sourceMap, sinkMap, edges, userCodeMap, uidNodeMap,
				deploymentType, monitoringProperties);
		}

		private boolean validateBackCouplingEdge(final Set<Node> visitedNodes, final Node currentNode,
				final Node destNode) {
			// implement detection of back coupling (cycle forming) edge!
			for (final Node n : currentNode.inputs) {
				// be careful, only reference comparison!
				if (destNode == n)
					return true;
				else if (!visitedNodes.contains(n)) {
					visitedNodes.add(n);
					if (validateBackCouplingEdge(visitedNodes, n, destNode))
						return true;
				}
			}
			return false;
		}
	}

	/**
     *
     */
	public static final class Node implements Visitable<Node>, Serializable {

		private static final long serialVersionUID = -1;

		// ---------------------------------------------------
		// Node Properties.
		// ---------------------------------------------------

		public static enum DataPersistenceType {

			EPHEMERAL,

			PERSISTED_IN_MEMORY,

			PERSISTED_RELIABLE
		}

		public static enum ExecutionType {

			BLOCKING,

			PIPELINED,
		}

		public static enum ExecutionEnvironment {

			SANDBOXED,

			NORMAL
		}

		// ---------------------------------------------------
		// Constructors.
		// ---------------------------------------------------

		public Node(final UUID uid, final String name) {
            this(uid, name, 1, 1, DataPersistenceType.EPHEMERAL, ExecutionType.PIPELINED);
		}

		public Node(final UUID uid, final String name, int degreeOfParallelism, int perWorkerParallelism) {
            this(uid, name, degreeOfParallelism, perWorkerParallelism, DataPersistenceType.EPHEMERAL,
                    ExecutionType.PIPELINED);
		}

		public Node(final UUID uid, final String name, int degreeOfParallelism,
				int perWorkerParallelism, final DataPersistenceType dataPersistenceType,
				final ExecutionType executionType) {
			// sanity check.
			if (uid == null)
				throw new IllegalArgumentException("uid == null");
			if (name == null)
				throw new IllegalArgumentException("name == null");
			if (degreeOfParallelism < 1)
				throw new IllegalArgumentException("degreeOfParallelism < 1");
			if (perWorkerParallelism < 1)
				throw new IllegalArgumentException("perWorkerParallelism < 1");
			if (dataPersistenceType == null)
				throw new IllegalArgumentException("dataPersistenceType == null");
			if (executionType == null)
				throw new IllegalArgumentException("executionType == null");

			this.uid = uid;

			this.name = name;

			this.degreeOfParallelism = degreeOfParallelism;

			this.perWorkerParallelism = perWorkerParallelism;

			this.inputs = new ArrayList<Node>();

			this.outputs = new ArrayList<Node>();

			this.executionNodes = new HashMap<UUID, ExecutionNode>();

			this.dataPersistenceType = dataPersistenceType;

			this.executionType = executionType;
		}

		// ---------------------------------------------------
		// Fields.
		// ---------------------------------------------------

		public final UUID uid;

		public final String name;

		public final int degreeOfParallelism;

		public final int perWorkerParallelism;

		public final DataPersistenceType dataPersistenceType;

		public final ExecutionType executionType;

		private final List<Node> inputs;

		private final List<Node> outputs;

		private final Map<UUID, ExecutionNode> executionNodes;

		// ---------------------------------------------------
		// Public.
		// ---------------------------------------------------

		public void addInput(final Node node) {
			// sanity check.
			if (node == null)
				throw new IllegalArgumentException("node == null");
			if (this == node)
				throw new IllegalArgumentException("self referencing node relations are not allowed");

			inputs.add(node);
		}

		public Collection<Node> getInputs() {
			return Collections.unmodifiableList(inputs);
		}

		public void addOutput(final Node node) {
			// sanity check.
			if (node == null)
				throw new IllegalArgumentException("node == null");
			if (this == node)
				throw new IllegalArgumentException("self referencing node relations are not allowed");

			outputs.add(node);
		}

		public Collection<Node> getOutputs() {
			return Collections.unmodifiableList(outputs);
		}

		public void addExecutionNode(final ExecutionNode exeNode) {
			// sanity check.
			if (exeNode == null)
				throw new IllegalArgumentException("exeNode == null");

			executionNodes.put(exeNode.uid, exeNode);
		}

		public List<ExecutionNode> getExecutionNodes() {
			return Collections.unmodifiableList(new ArrayList<ExecutionNode>(executionNodes.values()));
		}

		@Override
		public String toString() {
			return (new StringBuilder())
				.append("Node = {")
				.append(" name = " + name + ", ")
				.append(" }").toString();
		}

		public void accept(final Visitor<Node> visitor) {
			visitor.visit(this);
		}
	}

	/**
     *
     */
	public static final class ExecutionNode implements Visitable<ExecutionNode> {

		// ---------------------------------------------------
		// Constructors.
		// ---------------------------------------------------

		public ExecutionNode(final UUID uid, final int taskIndex, final Node logicalNode) {
			// sanity check.
			if (uid == null)
				throw new IllegalArgumentException("uid == null");
            if (taskIndex < 0)
                throw new IllegalArgumentException("taskIndex < 0");
			if (logicalNode == null)
				throw new IllegalArgumentException("logicalNode == null");

			this.uid = uid;

            this.taskIndex = taskIndex;

			this.logicalNode = logicalNode;
		}

		// ---------------------------------------------------
		// Fields.
		// ---------------------------------------------------

		public final UUID uid;

		public final Node logicalNode;

        public final int taskIndex;

		private TaskDescriptor taskDescriptor;

		private TaskBindingDescriptor taskBindingDescriptor;

		private TaskState currentState;

		// ---------------------------------------------------
		// Public.
		// ---------------------------------------------------

		public void setState(final TaskState state) {
			// sanity check.
			if (state == null)
				throw new IllegalArgumentException("state == null");

			currentState = state;
		}

		public TaskState getState() {
			return currentState;
		}

		public void setTaskDescriptor(final TaskDescriptor taskDescriptor) {
			// sanity check.
			if (taskDescriptor == null)
				throw new IllegalArgumentException("taskDescriptor == null");
			if (this.taskDescriptor != null)
				throw new IllegalStateException("taskDescriptor is already set");

			this.taskDescriptor = taskDescriptor;
		}

		public TaskDescriptor getTaskDescriptor() {
			return this.taskDescriptor;
		}

		public void setTaskBindingDescriptor(final TaskBindingDescriptor taskBindingDescriptor) {
			// sanity check.
			if (taskBindingDescriptor == null)
				throw new IllegalArgumentException("taskBindingDescriptor == null");
			if (this.taskBindingDescriptor != null)
				throw new IllegalStateException("taskBindingDescriptor is already set");

			this.taskBindingDescriptor = taskBindingDescriptor;
		}

		public TaskBindingDescriptor getTaskBindingDescriptor() {
			return this.taskBindingDescriptor;
		}

		@Override
		public String toString() {
			return (new StringBuilder())
				.append("ExecutionNode = {")
				.append(" uid = " + uid.toString() + ", ")
				.append(" taskDescriptor = " + taskDescriptor.toString() + ", ")
				.append(" taskBindingDescriptor = " + taskBindingDescriptor.toString())
				.append(" }").toString();
		}

		public void accept(final Visitor<ExecutionNode> visitor) {
			visitor.visit(this);
		}
	}

	/**
     *
     */
	public static final class Edge implements Serializable {

		private static final long serialVersionUID = 707567426961035903L;

		// ---------------------------------------------------
		// Edge Properties.
		// ---------------------------------------------------

		public static enum TransferType {

			POINT_TO_POINT,

			ALL_TO_ALL
		}

		public static enum EdgeType {

			FORWARD_EDGE,

			BACKWARD_EDGE
		}

		public static enum PartitioningType {

			NOT_PARTITIONED,

			HASH_PARTITIONED,

			RANGE_PARTITIONED,

			BROADCAST
		}

		// ---------------------------------------------------
		// Constructor.
		// ---------------------------------------------------

		public Edge(final Node srcNode,
				final Node dstNode,
				final TransferType transferType,
				final EdgeType edgeType) {

			// sanity check.
			if (srcNode == null)
				throw new IllegalArgumentException("srcNode == null");
			if (dstNode == null)
				throw new IllegalArgumentException("dstNode == null");
			if (transferType == null)
				throw new IllegalArgumentException("transferType == null");
			if (edgeType == null)
				throw new IllegalArgumentException("edgeType == null");

			this.srcNode = srcNode;

			this.dstNode = dstNode;

			this.transferType = transferType;

			this.edgeType = edgeType;
		}

		// ---------------------------------------------------
		// Fields.
		// ---------------------------------------------------

		public final Node srcNode;

		public final Node dstNode;

		public final TransferType transferType;

		public final EdgeType edgeType;

		// ---------------------------------------------------
		// Public.
		// ---------------------------------------------------

		@Override
		public String toString() {
			return (new StringBuilder())
				.append("Edge = {")
				.append(" srcNode = " + srcNode.toString() + ", ")
				.append(" dstNode = " + dstNode.toString() + ", ")
				.append(" transferType = " + transferType.toString() + ", ")
				.append(" edgeType = " + edgeType.toString() + ", ")
				.append(" }").toString();
		}
	}

	// ---------------------------------------------------
	// Utility Classes.
	// ---------------------------------------------------

	/**
     *
     */
	public static interface Visitor<T> {

		public abstract void visit(final T element);
	}

	/**
     *
     */
	public static interface Visitable<T> {

		public abstract void accept(final Visitor<T> visitor);
	}

	/**
     *
     */
	public static final class TopologyBreadthFirstTraverser {

		public static void traverse(final AuraTopology topology, final Visitor<Node> visitor) {
			traverse(false, topology, visitor);
		}

		public static void traverseBackwards(final AuraTopology topology, final Visitor<Node> visitor) {
			traverse(true, topology, visitor);
		}

		private static void traverse(final boolean traverseBackwards, final AuraTopology topology,
				final Visitor<Node> visitor) {
			// sanity check.
			if (topology == null)
				throw new IllegalArgumentException("topology == null");
			if (visitor == null)
				throw new IllegalArgumentException("visitor == null");

			final Set<Node> visitedNodes = new HashSet<Node>();
			final Queue<Node> q = new LinkedList<Node>();

			final Collection<Node> startNodes;
			if (traverseBackwards)
				startNodes = topology.sinkMap.values();
			else
				startNodes = topology.sourceMap.values();

			for (final Node node : startNodes)
				q.add(node);

			while (!q.isEmpty()) {
				final Node node = q.remove();
				node.accept(visitor);

				final Collection<Node> nextVisitedNodes;
				if (traverseBackwards)
					nextVisitedNodes = node.inputs;
				else
					nextVisitedNodes = node.outputs;

				for (final Node nextNode : nextVisitedNodes) {
					if (!visitedNodes.contains(nextNode)) {
						q.add(nextNode);
						visitedNodes.add(nextNode);
					}
				}
			}
		}
	}

	/**
     *
     */
	public static final class TopologyDepthFirstSearcher {

		// TODO: change to iterative implementation!

		public static boolean search(final Node start, final Node goal) {
			// sanity check.
			if (start == null)
				throw new IllegalArgumentException("start == null");
			if (goal == null)
				throw new IllegalArgumentException("goal == null");

			return searchHelper(new HashSet<Node>(), start, goal);
		}

		private static boolean searchHelper(final Set<Node> visitedNodes, final Node current, final Node goal) {
			visitedNodes.add(current);
			// be careful, only reference comparison!
			if (current == goal)
				return true;
			for (final Node n : current.outputs)
				if (!visitedNodes.contains(n))
					searchHelper(visitedNodes, n, goal);
			return false;
		}
	}
}
