package de.tuberlin.aura.workloadmanager;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDeploymentDescriptor;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.ExecutionNode;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.Node;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.TopologyBreadthFirstTraverser;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.Visitor;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.protocols.ClientWMProtocol;
import de.tuberlin.aura.core.protocols.WM2TMProtocol;
import de.tuberlin.aura.demo.deployment.LocalDeployment;

public class WorkloadManager implements ClientWMProtocol {

	//---------------------------------------------------
    // Constructors.
    //---------------------------------------------------
	
	public WorkloadManager( final MachineDescriptor machine ) {
		// sanity check.
		if( machine == null )
			throw new IllegalArgumentException( "machine == null" );
		
		this.machine = machine;
			
		this.ioManager = new IOManager( this.machine );
		
		this.rpcManager = new RPCManager( ioManager );
	
		this.workerMachines = new ArrayList<MachineDescriptor>();		
		workerMachines.add( LocalDeployment.MACHINE_1_DESCRIPTOR );
		workerMachines.add( LocalDeployment.MACHINE_2_DESCRIPTOR );
		workerMachines.add( LocalDeployment.MACHINE_3_DESCRIPTOR );
		workerMachines.add( LocalDeployment.MACHINE_4_DESCRIPTOR );
		
		rpcManager.registerRPCProtocolImpl( this, ClientWMProtocol.class );
		
		this.topologyParallelizer = new TopologyParallelizer();
	}

	//---------------------------------------------------
    // Fields.
    //--------------------------------------------------- 
	
	private static final Logger LOG = Logger.getLogger( WorkloadManager.class ); 
	
	private final MachineDescriptor machine;
	
	private final IOManager ioManager; 

	private final RPCManager rpcManager;
	
	private final List<MachineDescriptor> workerMachines;
	
	private final TopologyParallelizer topologyParallelizer;
	
	//---------------------------------------------------
    // Private.
    //---------------------------------------------------	

	// TODO: check if connections already exist.

	@Override
	public void submitTopology( final AuraTopology topology ) {
		// sanity check.
		if( topology == null )
			throw new IllegalArgumentException( "topology == null" );

		// Parallelizing.
		topologyParallelizer.parallelizeTopology( topology );
		
		// Scheduling.
		TopologyBreadthFirstTraverser.traverse( topology, new Visitor<Node>() {			
			
			private int machineIdx = 0;
			
			@Override
			public void visit( final Node element ) {
				for( final ExecutionNode en : element.getExecutionNodes() ) {	
					en.getTaskDescriptor().setMachineDescriptor( workerMachines.get( machineIdx ) );
				}
				++machineIdx;
			}
		} );	
		
		// Deploying. 
		TopologyBreadthFirstTraverser.traverseBackwards( topology, new Visitor<Node>() {			
			
			@Override
			public void visit( final Node element ) {				
				for( final ExecutionNode en : element.getExecutionNodes() ) {					
					final TaskDeploymentDescriptor tdd = 
							new TaskDeploymentDescriptor( en.getTaskDescriptor(), 
						 	 							  en.getTaskBindingDescriptor() );
					final WM2TMProtocol tmProtocol = 
							rpcManager.getRPCProtocolProxy( WM2TMProtocol.class, 
									 						en.getTaskDescriptor().getMachineDescriptor() );			
					tmProtocol.installTask( tdd );
					LOG.info( "deploy task : " + tdd.toString() );
				}
			}
		} );
	}
}
