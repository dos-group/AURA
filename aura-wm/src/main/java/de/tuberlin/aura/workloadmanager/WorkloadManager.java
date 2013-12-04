package de.tuberlin.aura.workloadmanager;

import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.directedgraph.AuraDirectedGraph.AuraTopology;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.protocols.ClientWMProtocol;
//import org.apache.log4j.Logger;

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
	
		rpcManager.registerRPCProtocolImpl( this, ClientWMProtocol.class );
	}

	//---------------------------------------------------
    // Fields.
    //--------------------------------------------------- 
	
	//private static final Logger LOG = Logger.getLogger( WorkloadManager.class ); 
	
	private final MachineDescriptor machine;
	
	public final IOManager ioManager; 

	public final RPCManager rpcManager;
	
	//---------------------------------------------------
    // Private.
    //---------------------------------------------------	

	// TODO: check if connections already exist.
	
	// TODO: detect failure if not serializable classes are sent over network 

	@Override
	public void submitTopology( final AuraTopology topology ) {
		// sanity check.
		if( topology == null )
			throw new IllegalArgumentException( "topology == null" );
		
		/*final List<WM2TMProtocol> tmList = new ArrayList<WM2TMProtocol>();
		for( final TaskDescriptor task : tasks ) {
			rpcManager.connectToMessageServer( task.machine );			
			tmList.add( rpcManager.getRPCProtocolProxy( WM2TMProtocol.class, task.machine ) );
		}
		
		for( int index = tasks.size() - 1; index >= 0; --index ) {
			
			final UserCode uc = userCode.get( index );
			final TaskDescriptor task = tasks.get( index );
			final TaskBindingDescriptor taskBinding = bindings.get( index );
			final WM2TMProtocol tmProtocol = tmList.get( index );
					
			tmProtocol.installTask( task, taskBinding, uc );
		}*/
	}
}
