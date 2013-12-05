package de.tuberlin.aura.core.directedgraph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.tuberlin.aura.core.common.utils.Pair;
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
			
				this.edges = new HashMap<String,String>();
				
				this.edgeProperties = new HashMap<Pair<String,String>,List<Object>>();
			}
		
			private final AuraTopologyBuilder tb;
			
			private final Map<String,String> edges;
			
			private final Map<Pair<String,String>,List<Object>> edgeProperties;
			
			private Node srcNode;
						
			public NodeConnector currentSource( final Node srcNode ) {
				this.srcNode = srcNode;
				return this;
			} 
			
			public AuraTopologyBuilder connectTo( final String dstNodeName, 
						  						  final Edge.TransferType transferType ) {
				return connectTo( dstNodeName, transferType, Edge.DataLifeTime.EPHEMERAL, Edge.ExecutionType.CONCURRENT );
			}
			
			public AuraTopologyBuilder connectTo( final String dstNodeName, 
				  	  							  final Edge.TransferType transferType, 
				  	  							  final Edge.DataLifeTime dataLifeTime ) {		
				return connectTo( dstNodeName, transferType, dataLifeTime, Edge.ExecutionType.CONCURRENT );
			}
			
			public AuraTopologyBuilder connectTo( final String dstNodeName, 
					  						  	  final Edge.TransferType transferType, 
					  						  	  final Edge.DataLifeTime dataLifeTime,
					  						  	  final Edge.ExecutionType executionType ) {
				// sanity check.
				if( dstNodeName == null )
					throw new IllegalArgumentException( "dstNode == null" );
				if( transferType == null )
					throw new IllegalArgumentException( "transferType == null" );
				if( dataLifeTime == null )
					throw new IllegalArgumentException( "dataLifeTime == null" );

				Object[] properties = { transferType, dataLifeTime, executionType }; 
				edges.put( srcNode.name, dstNodeName );
				edgeProperties.put( new Pair<String,String>( srcNode.name, dstNodeName ), Arrays.asList( properties ) );
				return tb;
			}
			
			public Map<String,String> getEdges() {
				return Collections.unmodifiableMap( edges );
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
			
			this.codeExtractor = codeExtractor;
						
			this.nodeConnector = new NodeConnector( this );
		}
		
		//---------------------------------------------------
	    // Fields.
	    //---------------------------------------------------
		
		public final Map<String,Node> nodeMap;
		
		public final Map<String,Node> sourceMap;

		public final Map<String,Node> sinkMap;
		
		public final List<Edge> edges;
		
		public final UserCodeExtractor codeExtractor;
		
		public final NodeConnector nodeConnector;
		
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
		
		public AuraTopology build() {					
			final Map<Pair<String,String>,List<Object>> edgeProperties = nodeConnector.getEdgeProperties();
			for( final Map.Entry<String,String> entry : nodeConnector.getEdges().entrySet() ) {
				
				final Node srcNode = nodeMap.get( entry.getKey() );
				final Node dstNode = nodeMap.get( entry.getValue() );
				final List<Object> properties = edgeProperties.get( new Pair<String,String>( entry.getKey(), entry.getValue() ) );
				final Edge.TransferType transferType = (Edge.TransferType) properties.get( 0 );
				final Edge.DataLifeTime dataLifeTime = (Edge.DataLifeTime) properties.get( 1 );
				final Edge.ExecutionType executionType = (Edge.ExecutionType) properties.get( 2 );
				
				srcNode.addOutput( dstNode );
				dstNode.addInput( srcNode );				
				final boolean isBackCouplingEdge = detectBackCoupling(); 			
				edges.add( new Edge( srcNode, dstNode, transferType, dataLifeTime, executionType, isBackCouplingEdge ) );	
			
				sourceMap.remove( dstNode.name );			
				sinkMap.remove( srcNode.name );
			}

			final Map<String,UserCode> userCodeMap = new HashMap<String,UserCode>();
			for( final Node n : nodeMap.values() ) {				
				final UserCode uc = codeExtractor.extractUserCodeClass( n.userClazz );
				userCodeMap.put( n.name, uc );
			}
			
			return new AuraTopology( nodeMap, sourceMap, sinkMap, edges, userCodeMap );
		}
	
		private boolean detectBackCoupling() {
			return false; // TODO: implement detection of back coupling (cycle forming) edge!
		}
	}
	
	/**
	 * 
	 */
	public static final class Node implements Serializable {

		private static final long serialVersionUID = -7726710143171176855L;
		
		//---------------------------------------------------
	    // Constructor.
	    //---------------------------------------------------

		public Node( final String name, final Class<?> userClazz ) {
			this( name, userClazz, 1 );
		} 
		
		public Node( final String name, final Class<?> userClazz, int degreeOfParallelism ) {
			// sanity check.
			if( name == null )
				throw new IllegalArgumentException( "name == null" );
			if( userClazz == null )
				throw new IllegalArgumentException( "userClazz == null" );
			if( degreeOfParallelism < 1 )
				throw new IllegalArgumentException( "degreeOfParallelism < 1" );
			
			this.name = name;
			
			this.userClazz = userClazz;
			
			this.degreeOfParallelism = degreeOfParallelism;
		
			this.inputs = new ArrayList<Node>();
			
			this.outputs = new ArrayList<Node>();		
		} 
		
		//---------------------------------------------------
	    // Fields.
	    //---------------------------------------------------
		
		public final String name;
		
		public final Class<?> userClazz;
		
		public final int degreeOfParallelism;
		
		public final List<Node> inputs;
		
		public final List<Node> outputs;
		
		//---------------------------------------------------
	    // Public.
	    //---------------------------------------------------
		
		public void addInput( final Node node ) {
			// sanity check.
			if( node == null )
				throw new IllegalArgumentException( "node == null" );
			
			inputs.add( node );
		}

		public void addOutput( final Node node ) {
			// sanity check.
			if( node == null )
				throw new IllegalArgumentException( "node == null" );
			
			outputs.add( node );
		}
		
		@Override
		public String toString() {
			return (new StringBuilder())
					.append( "Node = {" )
					.append( " name = " + name )
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
		
		public static enum ExecutionType {
			
			SEQUENTIAL,
			
			CONCURRENT
		}

		public static enum DataLifeTime {
			
			EPHEMERAL,
			
			PERSISTED,
			
			PERSISTED_RELIABLE 
		} 
		
		//---------------------------------------------------
	    // Constructor.
	    //---------------------------------------------------
		
		public Edge( final Node srcNode, 
					 final Node dstNode, 
					 final TransferType transferType,
					 final DataLifeTime dataLifeTime,
					 final ExecutionType dstExecutionType,
					 final boolean isBackCouplingEdge ) {
			
			// sanity check.
			if( srcNode ==  null )
				throw new IllegalArgumentException( "srcNode == null" );
			if( dstNode ==  null )
				throw new IllegalArgumentException( "dstNode == null" );
			if( transferType ==  null )
				throw new IllegalArgumentException( "transferType == null" );
			if( dataLifeTime ==  null )
				throw new IllegalArgumentException( "dataLifeTime == null" );
			if( dstExecutionType ==  null )
				throw new IllegalArgumentException( "dstExecutionType == null" );
			
			this.srcNode = srcNode;
			
			this.dstNode = dstNode;
			
			this.transferType = transferType;
			
			this.dataLifeTime = dataLifeTime;
			
			this.dstExecutionType = dstExecutionType;
			
			this.isBackCouplingEdge = isBackCouplingEdge;
		}

		//---------------------------------------------------
	    // Fields.
	    //---------------------------------------------------

		public final Node srcNode;
		
		public final Node dstNode;
		
		public final TransferType transferType;
		
		public final DataLifeTime dataLifeTime;
		
		public final ExecutionType dstExecutionType;
		
		public final boolean isBackCouplingEdge;
		
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
					.append( " dataLifeTime = " + dataLifeTime.toString() + ", " )
					.append( " dstExecutionType = " + dstExecutionType.toString() + ", " )
					.append( " isBackCouplingEdge = " + isBackCouplingEdge )
					.append( " }" ).toString();
		}
	}
	
	/**
	 * 
	 */
	public static interface Visitor<T> {
		
		public abstract void visit( final T element );
	}
	
	/**
	 * 
	 */
	public static final class TopologyVisitor implements Visitor<Node>{

		@Override
		public void visit( final Node element ) {			
		}
	}
	
	public static final class BreadthFirstTraverser {
	
		public BreadthFirstTraverser() {
		}
		
	
	}
}
