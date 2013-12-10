package de.tuberlin.aura.workloadmanager;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import de.tuberlin.aura.core.common.utils.Pair;
import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskBindingDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDescriptor;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.Node;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.TopologyBreadthFirstTraverser;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.Visitor;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.protocols.ClientWMProtocol;
import de.tuberlin.aura.core.protocols.WM2TMProtocol;
import de.tuberlin.aura.core.task.usercode.UserCode;
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
		
		//this.topologyParallelizer = new AuraTopologyParallelizer();
	}

	//---------------------------------------------------
    // Fields.
    //--------------------------------------------------- 
	
	//private static final Logger LOG = Logger.getLogger( WorkloadManager.class ); 
	
	private final MachineDescriptor machine;
	
	public final IOManager ioManager; 

	public final RPCManager rpcManager;
	
	public final List<MachineDescriptor> workerMachines;
	
	//private final AuraTopologyParallelizer topologyParallelizer;
	
	//---------------------------------------------------
    // Private.
    //---------------------------------------------------	

	// TODO: check if connections already exist.

	@Override
	public void submitTopology( final AuraTopology topology ) {
		// sanity check.
		if( topology == null )
			throw new IllegalArgumentException( "topology == null" );
						
		/*// TODO: debug output
		topologyParallelizer.parallelizeTopology( topology );
		TopologyBreadthFirstTraverser.traverse( topology, new Visitor<Node>() {			
			@Override
			public void visit( final Node element ) {
				
				LOG.info( "---------- " + element.name + " ----------" );
				for( final ExecutionNode en : element.getExecutionNodes() )
					LOG.info( en.toString() );
			}
		} );*/		
		
		
		final Map<UUID,TaskBindingDescriptor> taskBindingMap = new HashMap<UUID,TaskBindingDescriptor>();
		final Map<UUID,Pair<TaskDescriptor,UserCode>> taskMap = new HashMap<UUID,Pair<TaskDescriptor,UserCode>>();
		final Map<String,UUID> name2TaskIDMap = new HashMap<String,UUID>();
		
		final Deque<UUID> deploymentOrder = new ArrayDeque<UUID>();
		
		// First pass, create task descriptors (and later a clever worker machine assignment!). 
		TopologyBreadthFirstTraverser.traverse( topology, new Visitor<Node>() {

			private int machineIdx = 0;
			
			@Override
			public void visit( final Node element ) {				
				final UUID taskID = UUID.randomUUID();
				final UserCode uc = topology.userCodeMap.get( element.name );
				final TaskDescriptor td = new TaskDescriptor( taskID, element.name, uc );
				td.setMachineDescriptor( workerMachines.get( machineIdx++ ) );
				final Pair<TaskDescriptor,UserCode> taskAndUserCode = new Pair<TaskDescriptor,UserCode>( td, uc ); 								
				taskMap.put( taskID, taskAndUserCode );
				name2TaskIDMap.put( element.name, taskID );
				deploymentOrder.push( taskID );
			}
		} );
		
		// Second pass, create binding descriptors. 
		TopologyBreadthFirstTraverser.traverse( topology, new Visitor<Node>() {

			@Override
			public void visit( final Node element ) {
				
				final TaskDescriptor td = taskMap.get( name2TaskIDMap.get( element.name ) ).getFirst();
				final List<TaskDescriptor> inputs = new ArrayList<TaskDescriptor>();
				final List<TaskDescriptor> outputs = new ArrayList<TaskDescriptor>();
				
				for( final Node n : element.getInputs() )
					inputs.add( taskMap.get( name2TaskIDMap.get( n.name ) ).getFirst() );
				
				for( final Node n : element.getOutputs() )
					outputs.add( taskMap.get( name2TaskIDMap.get( n.name ) ).getFirst() );				
				
				final TaskBindingDescriptor tbd = new TaskBindingDescriptor( td, inputs, outputs );
				taskBindingMap.put( td.uid, tbd );
			}
		} );
		
		// Dummy Code...
		
		final List<WM2TMProtocol> tmList = new ArrayList<WM2TMProtocol>();
		for( final MachineDescriptor machine : workerMachines ) {		
			tmList.add( rpcManager.getRPCProtocolProxy( WM2TMProtocol.class, machine ) );
		}
		
		while( !deploymentOrder.isEmpty() ) {
			
			final WM2TMProtocol tmProtocol = tmList.get( deploymentOrder.size() - 1 );
			final UUID taskID = deploymentOrder.pop();
			final TaskDescriptor taskDescriptor = taskMap.get( taskID ).getFirst();			
			final TaskBindingDescriptor taskBinding = taskBindingMap.get( taskID );
						
			tmProtocol.installTask( taskDescriptor, taskBinding );
		}
	}
}
