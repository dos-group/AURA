package de.tuberlin.aura.workloadmanager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskBindingDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDescriptor;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.Edge;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.ExecutionNode;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.Node;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.TopologyBreadthFirstTraverser;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.Visitor;
import de.tuberlin.aura.core.task.usercode.UserCode;
import de.tuberlin.aura.workloadmanager.spi.ITopologyParallelizer;

public class TopologyParallelizer implements ITopologyParallelizer {
			
	//---------------------------------------------------
    // Inner Classes.
    //---------------------------------------------------
	
	@Override
	public void parallelizeTopology( final AuraTopology topology ) {
		// sanity check.
		if( topology == null )
			throw new IllegalArgumentException( "topology == null" );
		
		// First pass, create task descriptors (and later a clever worker machine assignment!). 
		TopologyBreadthFirstTraverser.traverse( topology, new Visitor<Node>() {
			
			@Override
			public void visit( final Node element ) {				
				final UserCode userCode = topology.userCodeMap.get( element.name );
				for( int index = 0; index < element.degreeOfParallelism; ++index ) {					
					final UUID taskID = UUID.randomUUID();
					final TaskDescriptor taskDescriptor = new TaskDescriptor( taskID, element.name, userCode );							
					final ExecutionNode executionNode = new ExecutionNode( element );					
					executionNode.setTaskDescriptor( taskDescriptor );					
					element.addExecutionNode( executionNode );
				}
			}
		} );
			
		// Second pass, create binding descriptors. 
		TopologyBreadthFirstTraverser.traverse( topology, new Visitor<Node>() {
			
			@Override
			public void visit( final Node element ) {
				
				// TODO: reduce code!
				
				// Bind inputs of the execution nodes. 
				final Map<UUID,List<TaskDescriptor>> executionNodeInputs = new HashMap<UUID,List<TaskDescriptor>>();
				for( final Node n : element.getInputs() ) {
					final Edge ie = topology.edges.get( new Pair<String,String>( n.name, element.name ) );					
					switch( ie.transferType ) {
						
						case ALL_TO_ALL: {		
							
							for( final ExecutionNode dstEN : element.getExecutionNodes() ) {
								final List<TaskDescriptor> inputDescriptors = new ArrayList<TaskDescriptor>();
								
								for( final ExecutionNode srcEN : n.getExecutionNodes() )
									inputDescriptors.add( srcEN.getTaskDescriptor() );			
								
								executionNodeInputs.put( dstEN.getTaskDescriptor().uid, 
										Collections.unmodifiableList( inputDescriptors ) ); 									
							}
							
						} break;
						
						case POINT_TO_POINT: {
							
							final int dstDegree = element.degreeOfParallelism;	
							final int srcDegree = n.degreeOfParallelism;	
							
							if( dstDegree >= srcDegree ) {
								
								final int numOfSrcLinks = dstDegree / srcDegree;
								final int numOfNodesWithOneAdditionalLink = dstDegree % srcDegree;
								final Iterator<ExecutionNode> dstIter = element.getExecutionNodes().iterator();
								
								int index = 0;				
								for( final ExecutionNode srcEN : n.getExecutionNodes() ) {
									
									final int numOfLinks = numOfSrcLinks + 
											( index++ < numOfNodesWithOneAdditionalLink ? 1 : 0 );
									
									int i = 0;
									while( i++ < numOfLinks ) {																	
										final ExecutionNode dstEN = dstIter.next();										
										final List<TaskDescriptor> inputDescriptors = new ArrayList<TaskDescriptor>();										
										inputDescriptors.add( srcEN.getTaskDescriptor() );										
										executionNodeInputs.put( dstEN.getTaskDescriptor().uid, 
												Collections.unmodifiableList( inputDescriptors ) );
									}
								}
								
							} else { // dstDegree < srcDegree
								
								final int numOfDstLinks = srcDegree / dstDegree; // number of links per dst execution node.								
								final int numOfNodesWithOneAdditionalLink = srcDegree % dstDegree;								
								final Iterator<ExecutionNode> srcIter = n.getExecutionNodes().iterator();
								
								int index = 0;				
								for( final ExecutionNode dstEN : element.getExecutionNodes() ) {
									
									final List<TaskDescriptor> inputDescriptors = new ArrayList<TaskDescriptor>();
									
									final int numOfLinks = numOfDstLinks + 
											( index++ < numOfNodesWithOneAdditionalLink ? 1 : 0 );
									
									int i = 0;
									while( i++ < numOfLinks ) {							
										final ExecutionNode srcEN = srcIter.next();
										inputDescriptors.add( srcEN.getTaskDescriptor() );
									}
									
									executionNodeInputs.put( dstEN.getTaskDescriptor().uid, 
											Collections.unmodifiableList( inputDescriptors ) );
								}								
							}							
							
						} break;						
					}
				}
				
				// Bind outputs of the execution nodes. 
				final Map<UUID,List<TaskDescriptor>> executionNodeOutputs = new HashMap<UUID,List<TaskDescriptor>>();
				for( final Node n : element.getOutputs() ) {
					final Edge ie = topology.edges.get( new Pair<String,String>( element.name, n.name ) );					
					switch( ie.transferType ) {
						
						case ALL_TO_ALL: {
							
							for( final ExecutionNode srcEN : element.getExecutionNodes() ) {
								final List<TaskDescriptor> outputDescriptors = new ArrayList<TaskDescriptor>();
								
								for( final ExecutionNode dstEN : n.getExecutionNodes() )
									outputDescriptors.add( dstEN.getTaskDescriptor() );			
								
								executionNodeOutputs.put( srcEN.getTaskDescriptor().uid, 
										Collections.unmodifiableList( outputDescriptors ) ); 									
							}
							
						} break;
						
						case POINT_TO_POINT: {
							
							final int dstDegree = n.degreeOfParallelism;	
							final int srcDegree = element.degreeOfParallelism;													
								
							if( dstDegree >= srcDegree ) {
								
								final int numOfSrcLinks = dstDegree / srcDegree;								
								final int numOfNodesWithOneAdditionalLink = dstDegree % srcDegree;								
								final Iterator<ExecutionNode> dstIter = n.getExecutionNodes().iterator();
								
								int index = 0;						
								for( final ExecutionNode srcEN : element.getExecutionNodes() ) {
									
									final List<TaskDescriptor> outputDescriptors = new ArrayList<TaskDescriptor>();
									
									final int numOfLinks = numOfSrcLinks + 
											( index++ < numOfNodesWithOneAdditionalLink ? 1 : 0 );
									
									int i = 0;
									while( i++ < numOfLinks ) {							
										final ExecutionNode dstEN = dstIter.next();
										outputDescriptors.add( dstEN.getTaskDescriptor() );
									}
									
									executionNodeOutputs.put( srcEN.getTaskDescriptor().uid, 
											Collections.unmodifiableList( outputDescriptors ) );
								}
				
							} else { // dstDegree < srcDegree
								
								final int numOfDstLinks = srcDegree / dstDegree; // number of links per dst execution node.								
								final int numOfNodesWithOneAdditionalLink = srcDegree % dstDegree;								
								final Iterator<ExecutionNode> srcIter = element.getExecutionNodes().iterator();
								
								int index = 0;				
								for( final ExecutionNode dstEN : n.getExecutionNodes() ) {
									
									final int numOfLinks = numOfDstLinks + 
											( index++ < numOfNodesWithOneAdditionalLink ? 1 : 0 );
									
									int i = 0;
									while( i++ < numOfLinks ) {																	
										final ExecutionNode srcEN = srcIter.next();										
										final List<TaskDescriptor> outputDescriptors = new ArrayList<TaskDescriptor>();										
										outputDescriptors.add( dstEN.getTaskDescriptor() );										
										executionNodeInputs.put( srcEN.getTaskDescriptor().uid, 
												Collections.unmodifiableList( outputDescriptors ) );
									}									
								}
							}

						} break;						
					}
				}

				// Assign the binding descriptors to the execution nodes.
				for( final ExecutionNode en : element.getExecutionNodes() ) {				
					
					List<TaskDescriptor> inputs = executionNodeInputs.get( en.getTaskDescriptor().uid );
					List<TaskDescriptor> outputs = executionNodeOutputs.get( en.getTaskDescriptor().uid );										
					
					final TaskBindingDescriptor bindingDescriptor = 
							new TaskBindingDescriptor( en.getTaskDescriptor(), 
													   inputs  != null ? inputs  : new ArrayList<TaskDescriptor>(),
													   outputs != null ? outputs : new ArrayList<TaskDescriptor>() );					
					
					en.setTaskBindingDescriptor( bindingDescriptor );
				}
			}
		} );
	}
}
