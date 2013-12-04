package de.tuberlin.aura.core.directedgraph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
			
			this.edges = Collections.unmodifiableList( edges );
			
			this.userCodeMap = Collections.unmodifiableMap( userCodeMap );
		}
		
		//---------------------------------------------------
	    // Fields.
	    //---------------------------------------------------
				
		public final Map<String,Node> nodeMap;
		
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
		
		public final class EdgeConnector {
			
			protected EdgeConnector( final AuraTopologyBuilder tb, final Node srcNode ) {				
				this.tb = tb;				
				this.srcNode = srcNode;
			}
		
			private final AuraTopologyBuilder tb;
			
			private final Node srcNode;
			
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
				
				final Node dstNode = nodeMap.get( dstNodeName );
				if( dstNode == null )
					throw new IllegalStateException( "dstNode == null" );
				
				srcNode.addOutput( dstNode );
				dstNode.addInput( srcNode );
				
				final boolean isBackCouplingEdge = detectBackCoupling(); 			
				edges.add( new Edge( srcNode, dstNode, transferType, dataLifeTime, executionType, isBackCouplingEdge ) );
				return tb;
			}
			
			private boolean detectBackCoupling() {
				return false; // TODO: implement detection of back coupling (cycle forming) edge!
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
			
			this.edges = new ArrayList<Edge>();
			
			this.codeExtractor = codeExtractor;
		}
		
		//---------------------------------------------------
	    // Fields.
	    //---------------------------------------------------
		
		public final Map<String,Node> nodeMap;
		
		public final List<Edge> edges;
		
		public final UserCodeExtractor codeExtractor;
		
		//---------------------------------------------------
	    // Public.
	    //---------------------------------------------------
		
		public EdgeConnector addNode( final Node node ) {
			// sanity check.
			if( node == null )
				throw new IllegalArgumentException( "node == null" );
			
			if( nodeMap.containsKey( node.name ) )
				throw new IllegalStateException( "node already exists" );
			
			nodeMap.put( node.name, node );	
			return new EdgeConnector( this, node );
		}
		
		public AuraTopology build() {					
			final Map<String,UserCode> userCodeMap = new HashMap<String,UserCode>();
			for( final Node n : nodeMap.values() ) {				
				final UserCode uc = codeExtractor.extractUserCodeClass( n.userClazz );
				userCodeMap.put( n.name, uc );
			}
			return new AuraTopology( nodeMap, edges, userCodeMap );			
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
		} 
		
		//---------------------------------------------------
	    // Fields.
	    //---------------------------------------------------
		
		public final String name;
		
		public final Class<?> userClazz;
		
		public final int degreeOfParallelism;
		
		public List<Node> inputs;
		
		public List<Node> outputs;
		
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
}
