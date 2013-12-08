package de.tuberlin.aura.core.directedgraph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskBindingDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDescriptor;
import de.tuberlin.aura.core.task.common.TaskStateMachine.TaskState;
import de.tuberlin.aura.core.task.usercode.UserCode;
import de.tuberlin.aura.core.task.usercode.UserCodeExtractor;

public class AuraDirectedGraph {

	// Disallow instantiation.
	private AuraDirectedGraph() {}
	
	/**
	 * 
	 */
	public static final class AuraTopology implements Serializable {
		
		private static final long serialVersionUID = 8527931035756128232L;
		
		//---------------------------------------------------
	    // Constructor.
	    //---------------------------------------------------
		
		public AuraTopology( final Map<String,Node> nodeMap, 
							 final Map<String,Node> sourceMap,
							 final Map<String,Node> sinkMap,
							 final List<Edge> edges, 
							 final Map<String,UserCode> userCodeMap ) {
			
			// sanity check.
			if( nodeMap == null )
				throw new IllegalArgumentException( "nodeMap == null" );
			if( sourceMap == null )
				throw new IllegalArgumentException( "sourceMap == null" );
			if( sinkMap == null )
				throw new IllegalArgumentException( "sinkMap == null" );		
			if( edges == null )
				throw new IllegalArgumentException( "edges == null" );
			if( userCodeMap == null )
				throw new IllegalArgumentException( "userCodeMap == null" );
			
			this.nodeMap = Collections.unmodifiableMap( nodeMap );
			
			this.sourceMap = Collections.unmodifiableMap( sourceMap );
			
			this.sinkMap = Collections.unmodifiableMap( sinkMap );
			
			this.edges = Collections.unmodifiableList( edges );
			
			this.userCodeMap = Collections.unmodifiableMap( userCodeMap );
		}
		
		//---------------------------------------------------
	    // Fields.
	    //---------------------------------------------------
				
		public final Map<String,Node> nodeMap;
		
		public final Map<String,Node> sourceMap;
		
		public final Map<String,Node> sinkMap;
		
		public final List<Edge> edges;
		
		public final Map<String,UserCode> userCodeMap;
	}
	
	/**
	 * 
	 */
	public static final class AuraTopologyBuilder {

		//---------------------------------------------------
	    // Inner Classes.
	    //---------------------------------------------------
		
		public final class NodeConnector {
			
			protected NodeConnector( final AuraTopologyBuilder tb ) {				
				this.tb = tb;							
				this.edges = new ArrayList<Pair<String,String>>();				
				this.edgeProperties = new HashMap<Pair<String,String>,List<Object>>();
			}
		
			private final AuraTopologyBuilder tb;			
			private final List<Pair<String,String>> edges;			
			private final Map<Pair<String,String>,List<Object>> edgeProperties;			
			private Node srcNode;
						
			public NodeConnector currentSource( final Node srcNode ) {
				this.srcNode = srcNode;
				return this;
			} 
						
			public AuraTopologyBuilder connectTo( final String dstNodeName, 
					  						  	  final Edge.TransferType transferType,
					  						  	  final Edge.EdgeType edgeType,
					  						  	  final Edge.DataPersistenceType dataLifeTime,
					  						  	  final Edge.ExecutionType executionType ) {
				// sanity check.
				if( dstNodeName == null )
					throw new IllegalArgumentException( "dstNode == null" );
				if( transferType == null )
					throw new IllegalArgumentException( "transferType == null" );
				if( edgeType == null )
					throw new IllegalArgumentException( "edgeType == null" );				
				if( dataLifeTime == null )
					throw new IllegalArgumentException( "dataLifeTime == null" );
				if( executionType == null )
					throw new IllegalArgumentException( "executionType == null" );

				Object[] properties = { transferType, edgeType, dataLifeTime, executionType }; 
				edges.add( new Pair<String,String>( srcNode.name, dstNodeName ) );
				edgeProperties.put( new Pair<String,String>( srcNode.name, dstNodeName ), Arrays.asList( properties ) );
				return tb;
			}
			
			public AuraTopologyBuilder connectTo( final String dstNodeName, final Edge.TransferType transferType ) {
				return connectTo( dstNodeName, transferType, Edge.EdgeType.FORWARD_EDGE, Edge.DataPersistenceType.EPHEMERAL, Edge.ExecutionType.CONCURRENT );
			}
			
			public AuraTopologyBuilder connectTo( final String dstNodeName, final Edge.TransferType transferType, final Edge.EdgeType edgeType ) {
				return connectTo( dstNodeName, transferType, edgeType, Edge.DataPersistenceType.EPHEMERAL, Edge.ExecutionType.CONCURRENT );
			}
			
			public AuraTopologyBuilder connectTo( final String dstNodeName, final Edge.TransferType transferType, final Edge.EdgeType edgeType, final Edge.DataPersistenceType dataLifeTime ) {		
				return connectTo( dstNodeName, transferType, edgeType, dataLifeTime, Edge.ExecutionType.CONCURRENT );
			}
			
			public List<Pair<String,String>> getEdges() {
				return Collections.unmodifiableList( edges );
			}
			
			public Map<Pair<String,String>,List<Object>> getEdgeProperties() {
				return Collections.unmodifiableMap( edgeProperties );
			}
		}
		
		//---------------------------------------------------
	    // Constructor.
	    //---------------------------------------------------

		public AuraTopologyBuilder( final UserCodeExtractor codeExtractor ) {			
			// sanity check.
			if( codeExtractor == null )
				throw new IllegalArgumentException( "codeExtractor == null" );
			
			this.nodeMap = new HashMap<String,Node>();
			
			this.sourceMap = new HashMap<String,Node>();
			
			this.sinkMap = new HashMap<String,Node>();
			
			this.edges = new ArrayList<Edge>();
			
			this.nodeConnector = new NodeConnector( this );

			this.codeExtractor = codeExtractor;
			
			this.userCodeMap = new HashMap<String,UserCode>();
		}
		
		//---------------------------------------------------
	    // Fields.
	    //---------------------------------------------------
		
		public final Map<String,Node> nodeMap;
		
		public final Map<String,Node> sourceMap;

		public final Map<String,Node> sinkMap;
		
		public final List<Edge> edges;
				
		public final NodeConnector nodeConnector;
		
		public final UserCodeExtractor codeExtractor;
		
		public final Map<String,UserCode> userCodeMap;
		
		public boolean isBuilded = false; 
		
		//---------------------------------------------------
	    // Public.
	    //---------------------------------------------------
		
		public NodeConnector addNode( final Node node ) {
			// sanity check.
			if( node == null )
				throw new IllegalArgumentException( "node == null" );
			
			if( nodeMap.containsKey( node.name ) )
				throw new IllegalStateException( "node already exists" );
			
			nodeMap.put( node.name, node );		
			sourceMap.put( node.name, node );
			sinkMap.put( node.name, node );
			
			return nodeConnector.currentSource( node );
		}
		
		public NodeConnector and() {
			return nodeConnector;
		}
		
		public AuraTopology build() {					
			
			if( !isBuilded ) {
			
				final Map<Pair<String,String>,List<Object>> edgeProperties = nodeConnector.getEdgeProperties();
				
				for( final Pair<String,String> entry : nodeConnector.getEdges() ) {
					final Node srcNode = nodeMap.get( entry.getFirst() );
					final Node dstNode = nodeMap.get( entry.getSecond() );								
					srcNode.addOutput( dstNode );
					dstNode.addInput( srcNode );				
				}
				
				for( final Pair<String,String> entry : nodeConnector.getEdges() ) {
					
					final Node srcNode = nodeMap.get( entry.getFirst() );
					final Node dstNode = nodeMap.get( entry.getSecond() );
					final List<Object> properties = edgeProperties.get( new Pair<String,String>( srcNode.name, dstNode.name ) );
					final Edge.TransferType transferType = (Edge.TransferType) properties.get( 0 );
					final Edge.EdgeType edgeType = (Edge.EdgeType) properties.get( 1 );				
					final Edge.DataPersistenceType dataLifeTime = (Edge.DataPersistenceType) properties.get( 2 );
					final Edge.ExecutionType executionType = (Edge.ExecutionType) properties.get( 3 );
					
					if( edgeType == Edge.EdgeType.BACKWARD_EDGE ) {
						if( !validateBackCouplingEdge( new HashSet<Node>(), srcNode, dstNode ) )
							throw new IllegalStateException( srcNode.name + " to " + dstNode.name + "is not a back coupling edge" );
					}
					
					edges.add( new Edge( srcNode, dstNode, transferType, edgeType, dataLifeTime, executionType ) );	
				
					if( edgeType != Edge.EdgeType.BACKWARD_EDGE ) {
						sourceMap.remove( dstNode.name );			
						sinkMap.remove( srcNode.name );
					}
				}
				
				for( final Node n : nodeMap.values() ) {				 
					final UserCode uc = codeExtractor.extractUserCodeClass( n.userClazz );
					userCodeMap.put( n.name, uc );
				}
				
				isBuilded = true;
			}
			
			return new AuraTopology( nodeMap, sourceMap, sinkMap, edges, userCodeMap );
		}
	
		private boolean validateBackCouplingEdge( final Set<Node> visitedNodes, final Node currentNode, final Node destNode ) {
			// implement detection of back coupling (cycle forming) edge!
			for( final Node n : currentNode.inputs ) {
				// be careful, only reference comparison!
				if( destNode == n )
					return true;
				else 
					if( !visitedNodes.contains( n ) ) {		
						visitedNodes.add( n );
						if( validateBackCouplingEdge( visitedNodes, n, destNode ) )
							return true;
					}
			}
			return false;
		}
	}
	
	/**
	 * 
	 */
	public static final class Node implements Serializable {

		private static final long serialVersionUID = -7726710143171176855L;
		
		//---------------------------------------------------
	    // Constructors.
	    //---------------------------------------------------

		public Node( final String name, final Class<?> userClazz ) {
			this( name, userClazz, 1, 1 );
		} 
		
		public Node( final String name, final Class<?> userClazz, int degreeOfParallelism, int perWorkerParallelism ) {
			// sanity check.
			if( name == null )
				throw new IllegalArgumentException( "name == null" );
			if( userClazz == null )
				throw new IllegalArgumentException( "userClazz == null" );
			if( degreeOfParallelism < 1 )
				throw new IllegalArgumentException( "degreeOfParallelism < 1" );
			if( perWorkerParallelism < 1 )
				throw new IllegalArgumentException( "perWorkerParallelism < 1" );
			
			this.name = name;
			
			this.userClazz = userClazz;
			
			this.degreeOfParallelism = degreeOfParallelism;
			
			this.perWorkerParallelism = perWorkerParallelism;
		
			this.inputs = new ArrayList<Node>();
			
			this.outputs = new ArrayList<Node>();
			
			this.executionNodes = new ArrayList<ExecutionNode>();
		} 
		
		//---------------------------------------------------
	    // Fields.
	    //---------------------------------------------------
		
		public final String name;
		
		public final Class<?> userClazz;
		
		public final int degreeOfParallelism;
		
		public final int perWorkerParallelism;
		
		public final List<Node> inputs;
		
		public final List<Node> outputs;
		
		public final List<ExecutionNode> executionNodes;
		
		//---------------------------------------------------
	    // Public.
	    //---------------------------------------------------
		
		public void addInput( final Node node ) {
			// sanity check.
			if( node == null )
				throw new IllegalArgumentException( "node == null" );
			if( this == node )
				throw new IllegalArgumentException( "self referencing node relations are not allowed" );
			
			inputs.add( node );
		}

		public void addOutput( final Node node ) {
			// sanity check.
			if( node == null )
				throw new IllegalArgumentException( "node == null" );
			if( this == node )
				throw new IllegalArgumentException( "self referencing node relations are not allowed" );
			
			outputs.add( node );
		}
		
		public void addExecutionNode( final ExecutionNode exeNode ) {
			// sanity check.
			if( exeNode == null )
				throw new IllegalArgumentException( "exeNode == null" );
			
			executionNodes.add( exeNode );
		}		
		
		@Override
		public String toString() {
			return (new StringBuilder())
					.append( "Node = {" )
					.append( " name = " + name + ", ")
					.append( " userClazz = " + userClazz.getCanonicalName() )
					.append( " }" ).toString();
		}
		
		public void accept( final Visitor<Node> visitor ) {			
			visitor.visit( this );			
		}		
	} 

	/**
	 * 
	 */
	public static final class ExecutionNode {

		//---------------------------------------------------
	    // Constructors.
	    //---------------------------------------------------

		public ExecutionNode( final Node logicalNode,
							  final TaskDescriptor taskDescriptor,
							  final TaskBindingDescriptor taskBindingDescriptor,
							  final UserCode userCode ) {
			
			// sanity check.
			if( logicalNode == null )
				throw new IllegalArgumentException( "logicalNode == null" );
			if( taskDescriptor == null )
				throw new IllegalArgumentException( "taskDescriptor == null" );
			if( taskBindingDescriptor == null )
				throw new IllegalArgumentException( "taskBindingDescriptor == null" );
			if( userCode == null )
				throw new IllegalArgumentException( "userCode == null" );
			
			this.logicalNode = logicalNode;
			
			this.taskDescriptor = taskDescriptor;
			
			this.taskBindingDescriptor = taskBindingDescriptor;
			
			this.userCode = userCode;
		}

		//---------------------------------------------------
	    // Fields.
	    //---------------------------------------------------

		public final Node logicalNode;
		
		public final TaskDescriptor taskDescriptor;
	
		public final TaskBindingDescriptor taskBindingDescriptor;
		
		public final UserCode userCode;
		
		private TaskState currentState;

		//---------------------------------------------------
	    // Public.
	    //---------------------------------------------------

		public void setState( final TaskState state ) {
			// sanity check.
			if( state == null )
				throw new IllegalArgumentException( "state == null" );
			
			currentState = state;
		}
		
		public TaskState getState() {
			return currentState;
		}
	}
	
	/**
	 * 
	 */
	public static final class Edge implements Serializable {
		
		private static final long serialVersionUID = 707567426961035903L;

		//---------------------------------------------------
	    // Edge Configurations.
	    //---------------------------------------------------

		public static enum TransferType {
			
			POINT_TO_POINT,
			
			ALL_TO_ALL,
			
			BROADCAST
		}
		
		public static enum EdgeType {
			
			FORWARD_EDGE,
			
			BACKWARD_EDGE 
		}

		public static enum DataPersistenceType {
			
			EPHEMERAL,
			
			PERSISTED_IN_MEMORY,
			
			PERSISTED_RELIABLE 
		} 
		
		public static enum ExecutionType {
			
			SEQUENTIAL,
			
			CONCURRENT,
			
			LAZY
		}
		
		//---------------------------------------------------
	    // Constructor.
	    //---------------------------------------------------
		
		public Edge( final Node srcNode, 
					 final Node dstNode, 
					 final TransferType transferType,
					 final EdgeType edgeType,
					 final DataPersistenceType dataLifeTime,
					 final ExecutionType dstExecutionType ) {
			
			// sanity check.
			if( srcNode ==  null )
				throw new IllegalArgumentException( "srcNode == null" );
			if( dstNode ==  null )
				throw new IllegalArgumentException( "dstNode == null" );
			if( transferType ==  null )
				throw new IllegalArgumentException( "transferType == null" );
			if( edgeType ==  null )
				throw new IllegalArgumentException( "edgeType == null" );
			if( dataLifeTime ==  null )
				throw new IllegalArgumentException( "dataLifeTime == null" );
			if( dstExecutionType ==  null )
				throw new IllegalArgumentException( "dstExecutionType == null" );
			
			this.srcNode = srcNode;
			
			this.dstNode = dstNode;
			
			this.transferType = transferType;
			
			this.edgeType = edgeType;
			
			this.dataLifeTime = dataLifeTime;
			
			this.dstExecutionType = dstExecutionType;
		}

		//---------------------------------------------------
	    // Fields.
	    //---------------------------------------------------

		public final Node srcNode;
		
		public final Node dstNode;
		
		public final TransferType transferType;
		
		public final EdgeType edgeType;
		
		public final DataPersistenceType dataLifeTime;
		
		public final ExecutionType dstExecutionType;
		
		//---------------------------------------------------
	    // Public.
	    //---------------------------------------------------
		
		@Override
		public String toString() {
			return (new StringBuilder())
					.append( "Edge = {" )
					.append( " srcNode = " + srcNode.toString() + ", " )
					.append( " dstNode = " + dstNode.toString() + ", " )
					.append( " transferType = " + transferType.toString() + ", " )
					.append( " edgeType = " + edgeType.toString() + ", " )
					.append( " dataLifeTime = " + dataLifeTime.toString() + ", " )
					.append( " dstExecutionType = " + dstExecutionType.toString() )
					.append( " }" ).toString();
		}
	}
	
	//---------------------------------------------------
    // Utility Classes.
    //---------------------------------------------------
	
	/**
	 * 
	 */
	public static interface Visitor<T> {
		
		public abstract void visit( final T element );
	}
	
	/**
	 * 
	 */
	public static final class TopologyBreadthFirstTraverser {
	
		public static void traverse( final AuraTopology topology, final Visitor<Node> visitor ) {
			// sanity check.
			if( topology == null )
				throw new IllegalArgumentException( "topology == null" );
			if( visitor == null )
				throw new IllegalArgumentException( "visitor == null" );
			
			final Set<Node> visitedNodes = new HashSet<Node>();									
			final Queue<Node> q = new LinkedList<Node>();

			for( final Node node : topology.sourceMap.values() )
				q.add( node );
			
			while(! q.isEmpty() ) { 
				final Node node = q.remove();
				node.accept( visitor );				
				for( final Node nextNode : node.outputs ) {
					if( !visitedNodes.contains( nextNode ) ) {
						q.add( nextNode );
						visitedNodes.add( nextNode );
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
		
		public static boolean search( final Node start, final Node goal ) {
			// sanity check.	
			if( start == null )
				throw new IllegalArgumentException( "start == null" );
			if( goal == null )
				throw new IllegalArgumentException( "goal == null" );
			
			return searchHelper( new HashSet<Node>(), start, goal );
		}	
		
		private static boolean searchHelper( final Set<Node> visitedNodes, final Node current, final Node goal ) {
			visitedNodes.add( current );
			// be careful, only reference comparison! 
			if( current == goal )
				return true;			
			for( final Node n : current.outputs )
				if( !visitedNodes.contains( n ) )
					searchHelper( visitedNodes, n, goal );
			return false;
		}
	}
}