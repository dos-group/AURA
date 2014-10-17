package de.tuberlin.aura.core.topology;

import java.io.Serializable;
import java.util.*;

import de.tuberlin.aura.core.common.utils.IVisitable;
import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.descriptors.Descriptors.AbstractNodeDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.NodeBindingDescriptor;
import de.tuberlin.aura.core.dataflow.api.DataflowNodeProperties;
import de.tuberlin.aura.core.record.tuples.AbstractTuple;
import de.tuberlin.aura.core.taskmanager.common.TaskStates.TaskState;
import de.tuberlin.aura.core.taskmanager.usercode.UserCode;
import de.tuberlin.aura.core.taskmanager.usercode.UserCodeExtractor;

public class Topology {

    // Disallow instantiation.
    private Topology() {}

    //---------------------------------------------------------------------------------------------------------------

    /**
     *
     */
    public static final class AuraTopology implements java.io.Serializable {

        private static final long serialVersionUID = -1L;

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        public final UUID machineID;

        public final String name;

        public UUID topologyID;

        public final Map<String, LogicalNode> nodeMap;

        public final Map<String, LogicalNode> sourceMap;

        public final Map<String, LogicalNode> sinkMap;

        public final Map<Pair<String, String>, Edge> edges;

        public final Map<String, List<UserCode>> userCodeMap;

        public final Map<UUID, LogicalNode> uidNodeMap;

        public final boolean isReExecutable;

        public Map<UUID, ExecutionNode> executionNodeMap;

        // ---------------------------------------------------
        // Constructor.
        // ---------------------------------------------------

        public AuraTopology(final UUID machineID,
                            final String name,
                            final UUID topologyID,
                            final Map<String, LogicalNode> nodeMap,
                            final Map<String, LogicalNode> sourceMap,
                            final Map<String, LogicalNode> sinkMap,
                            final Map<Pair<String, String>, Edge> edges,
                            final Map<String, List<UserCode>> userCodeMap,
                            final Map<UUID, LogicalNode> uidNodeMap,
                            final boolean isReExecutable) {

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

            this.machineID = machineID;

            this.name = name;

            this.topologyID = topologyID;

            this.nodeMap = nodeMap;

            this.sourceMap = sourceMap;

            this.sinkMap = sinkMap;

            this.edges = edges;

            this.userCodeMap = userCodeMap;

            this.uidNodeMap = uidNodeMap;

            this.isReExecutable = isReExecutable;

            this.executionNodeMap = null;
        }

        // ---------------------------------------------------
        // Public Methods.
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
        // Fields.
        // ---------------------------------------------------

        private final UUID machineID;

        private final Map<String, LogicalNode> nodeMap;

        private final Map<String, LogicalNode> sourceMap;

        private final Map<String, LogicalNode> sinkMap;

        private final Map<Pair<String, String>, Edge> edges;

        private final NodeConnector nodeConnector;

        private final UserCodeExtractor codeExtractor;

        private final Map<String, List<UserCode>> userCodeMap;

        private final Map<UUID, LogicalNode> uidNodeMap;

        private boolean isBuilt = false;

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

            this.nodeMap = new HashMap<>();

            this.sourceMap = new HashMap<>();

            this.sinkMap = new HashMap<>();

            this.edges = new HashMap<>();

            this.nodeConnector = new NodeConnector(this);

            this.codeExtractor = codeExtractor;

            this.userCodeMap = new HashMap<>();

            this.uidNodeMap = new HashMap<>();
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        public NodeConnector addNode(final LogicalNode node) {
            // sanity check.
            if (node == null)
                throw new IllegalArgumentException("node == null");
            if (nodeMap.containsKey(node.name))
                throw new IllegalStateException("node already exists");

            nodeMap.put(node.name, node);
            sourceMap.put(node.name, node);
            sinkMap.put(node.name, node);
            uidNodeMap.put(node.uid, node);
            return nodeConnector.currentSource(node);
        }

        public AuraTopology build(final String name, final boolean isReExecutable) {
            // sanity check.
            if (name == null)
                throw new IllegalArgumentException("name == null");

            if (!isBuilt) {

                final Map<Pair<String, String>, List<Object>> edgeProperties = nodeConnector.getEdgeProperties();

                for (final Pair<String, String> entry : nodeConnector.getEdges()) {
                    final LogicalNode srcNode = nodeMap.get(entry.getFirst());
                    final LogicalNode dstNode = nodeMap.get(entry.getSecond());
                    srcNode.addOutput(dstNode);
                    dstNode.addInput(srcNode);
                }

                for (final Pair<String, String> entry : nodeConnector.getEdges()) {

                    final LogicalNode srcNode = nodeMap.get(entry.getFirst());
                    final LogicalNode dstNode = nodeMap.get(entry.getSecond());
                    final List<Object> properties = edgeProperties.get(new Pair<>(srcNode.name, dstNode.name));
                    final Edge.TransferType transferType = (Edge.TransferType) properties.get(0);
                    final Edge.EdgeType edgeType = (Edge.EdgeType) properties.get(1);

                    if (edgeType == Edge.EdgeType.BACKWARD_EDGE) {
                        if (!validateBackCouplingEdge(new HashSet<LogicalNode>(), srcNode, dstNode))
                            throw new IllegalStateException(srcNode.name + " to " + dstNode.name + "is not a back coupling edge");
                    }

                    edges.put(new Pair<>(srcNode.name, dstNode.name), new Edge(srcNode, dstNode, transferType, edgeType));

                    if (edgeType != Edge.EdgeType.BACKWARD_EDGE) {
                        sourceMap.remove(dstNode.name);
                        sinkMap.remove(srcNode.name);
                    }
                }

                for (final LogicalNode n : nodeMap.values()) {
                    if (n instanceof InvokeableNode || n instanceof LogicalNode) {

                        final List<Class<?>> userCodeClazzList = new ArrayList<>();

                        for (DataflowNodeProperties nodeProperties : n.propertiesList) {
                            try {
                                if (nodeProperties.functionTypeName != null) {
                                    userCodeClazzList.add(Class.forName(nodeProperties.functionTypeName));
                                }
                            } catch (ClassNotFoundException e) {
                                throw new IllegalStateException("UDF type not found.");
                            }
                        }

                        final List<UserCode> userCodeList = new ArrayList<>();
                        for (final Class<?> userCodeClazz : userCodeClazzList) {
                            if (userCodeClazz != null && !AbstractTuple.class.isAssignableFrom(userCodeClazz)) { // TODO: shit hack...
                                final UserCode uc = codeExtractor.extractUserCodeClass(userCodeClazz);
                                userCodeList.add(uc);
                            }
                        }
                        userCodeMap.put(n.name, userCodeList);
                    }
                }

                isBuilt = true;
            }

            // Every call to build gives us the same topology with a new id.
            final UUID topologyID = UUID.randomUUID();

            return new AuraTopology(machineID,
                                    name,
                                    topologyID,
                                    nodeMap,
                                    sourceMap,
                                    sinkMap,
                                    edges,
                                    userCodeMap,
                                    uidNodeMap,
                                    isReExecutable);
        }

        private boolean validateBackCouplingEdge(final Set<LogicalNode> visitedNodes, final LogicalNode currentNode, final LogicalNode destNode) {
            // implement detection of back coupling (cycle forming) edge!
            for (final LogicalNode n : currentNode.inputs) {
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

        public NodeConnector and() {
            return nodeConnector;
        }

        public AuraTopology build(final String name) {
            return build(name, false);
        }

        // ---------------------------------------------------
        // Inner Classes.
        // ---------------------------------------------------

        public final class NodeConnector {

            // ---------------------------------------------------
            // Fields.
            // ---------------------------------------------------

            private final AuraTopologyBuilder tb;

            private final List<Pair<String, String>> edges;

            private final Map<Pair<String, String>, List<Object>> edgeProperties;

            private LogicalNode srcNode;

            // ---------------------------------------------------
            // Constructor.
            // ---------------------------------------------------

            protected NodeConnector(final AuraTopologyBuilder tb) {
                this.tb = tb;
                this.edges = new ArrayList<>();
                this.edgeProperties = new HashMap<>();
            }

            // ---------------------------------------------------
            // Public Methods.
            // ---------------------------------------------------

            public NodeConnector currentSource(final LogicalNode srcNode) {
                this.srcNode = srcNode;
                return this;
            }

            public AuraTopologyBuilder noConnects() {
                return tb;
            }

            public AuraTopologyBuilder connectTo(final String dstNodeName,
                                                 final Edge.TransferType transferType,
                                                 final Edge.EdgeType edgeType,
                                                 final LogicalNode.DataPersistenceType dataLifeTime,
                                                 final LogicalNode.ExecutionType executionType) {
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

                Object[] properties = {transferType, edgeType, dataLifeTime, executionType};
                edges.add(new Pair<>(srcNode.name, dstNodeName));
                edgeProperties.put(new Pair<>(srcNode.name, dstNodeName), Arrays.asList(properties));
                return tb;
            }

            public AuraTopologyBuilder connectTo(final String dstNodeName, final Edge.TransferType transferType) {
                return connectTo(dstNodeName,
                                 transferType,
                                 Edge.EdgeType.FORWARD_EDGE,
                                 LogicalNode.DataPersistenceType.EPHEMERAL,
                                 LogicalNode.ExecutionType.PIPELINED);
            }

            public AuraTopologyBuilder connectTo(final String dstNodeName, final Edge.TransferType transferType, final Edge.EdgeType edgeType) {
                return connectTo(dstNodeName, transferType, edgeType, LogicalNode.DataPersistenceType.EPHEMERAL, LogicalNode.ExecutionType.PIPELINED);
            }

            public AuraTopologyBuilder connectTo(final String dstNodeName,
                                                 final Edge.TransferType transferType,
                                                 final Edge.EdgeType edgeType,
                                                 final LogicalNode.DataPersistenceType dataLifeTime) {
                return connectTo(dstNodeName, transferType, edgeType, dataLifeTime, LogicalNode.ExecutionType.PIPELINED);
            }

            public List<Pair<String, String>> getEdges() {
                return Collections.unmodifiableList(edges);
            }

            public Map<Pair<String, String>, List<Object>> getEdgeProperties() {
                return Collections.unmodifiableMap(edgeProperties);
            }
        }
    }

    //---------------------------------------------------------------------------------------------------------------

    /**
     *
     */
    public static class LogicalNode implements Serializable, IVisitable<LogicalNode> {

        private static final long serialVersionUID = -1L;

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
        // Fields.
        // ---------------------------------------------------

        public final UUID uid;

        public final String name;

        public final int degreeOfParallelism;

        public final int perWorkerParallelism;

        public final DataPersistenceType dataPersistenceType;

        public final ExecutionType executionType;

        public final List<LogicalNode> inputs;

        public final List<LogicalNode> outputs;

        private final Map<UUID, ExecutionNode> executionNodes;

        public boolean isAlreadyDeployed = false;

        public final List<DataflowNodeProperties> propertiesList;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public LogicalNode(final UUID uid, final String name) {
            this(uid, name, 1, 1, DataPersistenceType.EPHEMERAL, ExecutionType.PIPELINED, (DataflowNodeProperties)null);
        }

        public LogicalNode(final UUID uid, final String name, int degreeOfParallelism, int perWorkerParallelism) {
            this(uid, name, degreeOfParallelism, perWorkerParallelism, DataPersistenceType.EPHEMERAL, ExecutionType.PIPELINED, (DataflowNodeProperties)null);
        }

        public LogicalNode(final UUID uid,
                           final String name,
                           int degreeOfParallelism,
                           int perWorkerParallelism,
                           final DataPersistenceType dataPersistenceType,
                           final ExecutionType executionType,
                           final DataflowNodeProperties properties) {
            this(uid, name, degreeOfParallelism, perWorkerParallelism, DataPersistenceType.EPHEMERAL, ExecutionType.PIPELINED, Arrays.asList(properties));
        }


        public LogicalNode(final UUID uid,
                           final String name,
                           int degreeOfParallelism,
                           int perWorkerParallelism,
                           final DataPersistenceType dataPersistenceType,
                           final ExecutionType executionType,
                           final List<DataflowNodeProperties> propertiesList) {
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

            this.inputs = new ArrayList<>();

            this.outputs = new ArrayList<>();

            this.executionNodes = new HashMap<>();

            this.dataPersistenceType = dataPersistenceType;

            this.executionType = executionType;

            this.propertiesList = propertiesList;
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        public void addInput(final LogicalNode node) {
            // sanity check.
            if (node == null)
                throw new IllegalArgumentException("node == null");
            if (this == node)
                throw new IllegalArgumentException("self referencing node relations are not allowed");

            inputs.add(node);
        }

        public List<LogicalNode> getInputs() {
            return Collections.unmodifiableList(inputs);
        }

        public void addOutput(final LogicalNode node) {
            // sanity check.
            if (node == null)
                throw new IllegalArgumentException("node == null");
            if (this == node)
                throw new IllegalArgumentException("self referencing node relations are not allowed");

            outputs.add(node);
        }

        public List<LogicalNode> getOutputs() {
            return Collections.unmodifiableList(outputs);
        }

        public void addExecutionNode(final ExecutionNode exeNode) {
            // sanity check.
            if (exeNode == null)
                throw new IllegalArgumentException("exeNode == null");

            executionNodes.put(exeNode.uid, exeNode);
        }

        public List<ExecutionNode> getExecutionNodes() {
            return Collections.unmodifiableList(new ArrayList<>(executionNodes.values()));
        }

        @Override
        public String toString() {
            return (new StringBuilder()).append("Node = {").append(" name = " + name + ", ").append(" }").toString();
        }

        public void accept(final IVisitor<LogicalNode> visitor) {
            visitor.visit(this);
        }
    }

    //---------------------------------------------------------------------------------------------------------------

    /**
     *
     */
    public static final class DatasetNode extends LogicalNode {

        public DatasetNode(final DataflowNodeProperties properties) {
            super(properties.operatorUID, properties.instanceName, properties.globalDOP, properties.localDOP, DataPersistenceType.EPHEMERAL, ExecutionType.PIPELINED, properties);
        }
    }

    /**
     *
     */
    public static final class OperatorNode extends LogicalNode {

        public OperatorNode(final List<DataflowNodeProperties> propertyList) {
            super(propertyList.get(0).operatorUID,
                  propertyList.get(0).instanceName,
                  propertyList.get(0).globalDOP,
                  propertyList.get(0).localDOP,
                  DataPersistenceType.EPHEMERAL,
                  ExecutionType.PIPELINED,
                  propertyList);
        }

        public OperatorNode(final DataflowNodeProperties properties) {
            super(properties.operatorUID, properties.instanceName, properties.globalDOP, properties.localDOP, DataPersistenceType.EPHEMERAL, ExecutionType.PIPELINED, properties);
        }
    }

    public static final class InvokeableNode extends LogicalNode {

        public InvokeableNode(final UUID uid, final String name, final int degreeOfParallelism, final int perWorkerParallelism, String udfTypeName) {

            super(uid, name, degreeOfParallelism, perWorkerParallelism, DataPersistenceType.EPHEMERAL, ExecutionType.PIPELINED,
                    new DataflowNodeProperties(uid, null, name, degreeOfParallelism, perWorkerParallelism, null,
                            null, null, null, null, udfTypeName, null, null, null, null, null, null, null, null, null));

        }
    }


    //---------------------------------------------------------------------------------------------------------------

    /**
     *
     */
    public static final class ExecutionNode implements Serializable, IVisitable<ExecutionNode> {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        public final UUID uid;

        public final LogicalNode logicalNode;

        public final int taskIndex;

        private AbstractNodeDescriptor nodeDescriptor;

        private NodeBindingDescriptor nodeBindingDescriptor;

        private TaskState currentState;

        // ---------------------------------------------------
        // Constructors.
        // ---------------------------------------------------

        public ExecutionNode(final UUID uid, final int taskIndex, final LogicalNode logicalNode) {
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
        // Public Methods.
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

        public void setNodeDescriptor(final AbstractNodeDescriptor nodeDescriptor) {
            // sanity check.
            if (nodeDescriptor == null)
                throw new IllegalArgumentException("nodeDescriptor == null");
            if (this.nodeDescriptor != null)
                throw new IllegalStateException("nodeDescriptor is already set");

            this.nodeDescriptor = nodeDescriptor;
        }

        public AbstractNodeDescriptor getNodeDescriptor() {
            return this.nodeDescriptor;
        }

        public void setNodeBindingDescriptor(final Descriptors.NodeBindingDescriptor nodeBindingDescriptor) {
            // sanity check.
            if (nodeBindingDescriptor == null)
                throw new IllegalArgumentException("nodeBindingDescriptor == null");

            this.nodeBindingDescriptor = nodeBindingDescriptor;
        }

        public Descriptors.NodeBindingDescriptor getNodeBindingDescriptor() {
            return this.nodeBindingDescriptor;
        }

        @Override
        public String toString() {
            return (new StringBuilder()).append("ExecutionNode = {")
                                        .append(" uid = " + uid.toString() + ", ")
                                        .append(" nodeDescriptor = " + nodeDescriptor.toString() + ", ")
                                        .append(" nodeBindingDescriptor = " + nodeBindingDescriptor.toString())
                                        .append(" }")
                                        .toString();
        }

        public void accept(final IVisitor<ExecutionNode> visitor) {
            visitor.visit(this);
        }
    }

    /**
     *
     */
    public static final class Edge implements Serializable {

        private static final long serialVersionUID = -1L;

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
        // Fields.
        // ---------------------------------------------------

        public final LogicalNode srcNode;

        public final LogicalNode dstNode;

        public final TransferType transferType;

        public final EdgeType edgeType;

        // ---------------------------------------------------
        // Constructor.
        // ---------------------------------------------------

        public Edge(final LogicalNode srcNode, final LogicalNode dstNode, final TransferType transferType, final EdgeType edgeType) {

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
        // Public Methods.
        // ---------------------------------------------------

        @Override
        public String toString() {
            return (new StringBuilder()).append("Edge = {")
                                        .append(" srcNode = " + srcNode.toString() + ", ")
                                        .append(" dstNode = " + dstNode.toString() + ", ")
                                        .append(" transferType = " + transferType.toString() + ", ")
                                        .append(" edgeType = " + edgeType.toString() + ", ")
                                        .append(" }")
                                        .toString();
        }
    }

    // ---------------------------------------------------
    // Utility Classes.
    // ---------------------------------------------------

    /**
     *
     */
    public static final class TopologyBreadthFirstTraverser {

        public static void traverse(final AuraTopology topology, final IVisitor<LogicalNode> visitor) {
            traverse(false, topology, visitor);
        }

        public static void traverseBackwards(final AuraTopology topology, final IVisitor<LogicalNode> visitor) {
            traverse(true, topology, visitor);
        }

        private static void traverse(final boolean traverseBackwards, final AuraTopology topology, final IVisitor<LogicalNode> visitor) {
            // sanity check.
            if (topology == null)
                throw new IllegalArgumentException("topology == null");
            if (visitor == null)
                throw new IllegalArgumentException("visitor == null");

            final Set<LogicalNode> visitedNodes = new HashSet<>();
            final Queue<LogicalNode> q = new LinkedList<>();

            final Collection<LogicalNode> startNodes;
            if (traverseBackwards)
                startNodes = topology.sinkMap.values();
            else
                startNodes = topology.sourceMap.values();

            for (final LogicalNode node : startNodes)
                q.add(node);

            while (!q.isEmpty()) {
                final LogicalNode node = q.remove();
                node.accept(visitor);

                final Collection<LogicalNode> nextVisitedNodes;
                if (traverseBackwards)
                    nextVisitedNodes = node.inputs;
                else
                    nextVisitedNodes = node.outputs;

                for (final LogicalNode nextNode : nextVisitedNodes) {
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

        public static boolean search(final LogicalNode start, final LogicalNode goal) {
            // sanity check.
            if (start == null)
                throw new IllegalArgumentException("start == null");
            if (goal == null)
                throw new IllegalArgumentException("goal == null");

            return searchHelper(new HashSet<LogicalNode>(), start, goal);
        }

        private static boolean searchHelper(final Set<LogicalNode> visitedNodes, final LogicalNode current, final LogicalNode goal) {
            visitedNodes.add(current);
            // be careful, only reference comparison!
            if (current == goal)
                return true;
            for (final LogicalNode n : current.outputs)
                if (!visitedNodes.contains(n))
                    searchHelper(visitedNodes, n, goal);
            return false;
        }
    }

    /**
     *
     */
    public static final class TopologyPrinter {

        // ---------------------------------------------------
        // Constructor.
        // ---------------------------------------------------

        private TopologyPrinter() {
        }

        // ---------------------------------------------------
        // Public Static Methods.
        // ---------------------------------------------------

        public static void printTopology(final AuraTopology topology) {
            // sanity check.
            if (topology == null)
                throw new IllegalArgumentException("topology == null");

            TopologyBreadthFirstTraverser.traverse(topology, new IVisitor<LogicalNode>() {

                @Override
                public void visit(LogicalNode element) {
                    System.out.println(element.name);
                }
            });
        }
    }
}
