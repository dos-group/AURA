package de.tuberlin.aura.workloadmanager;

import java.util.ArrayList;
import java.util.List;

//import org.apache.log4j.Logger;

import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskBindingDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDescriptor;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.protocols.ClientWMProtocol;
import de.tuberlin.aura.core.protocols.WM2TMProtocol;
import de.tuberlin.aura.core.task.usercode.UserCode;
import de.tuberlin.aura.core.task.usercode.UserCodeExtractor;

public class WorkloadManager implements ClientWMProtocol {

	//---------------------------------------------------
    // Constructors.
    //---------------------------------------------------
	
	public WorkloadManager( final MachineDescriptor machine ) {
		// sanity check.
		if( machine == null )
			throw new IllegalArgumentException( "machine must not be null" );
		
		this.machine = machine;
			
		this.ioManager = new IOManager( this.machine );
		
		this.rpcManager = new RPCManager( ioManager );
		
		this.codeExtractor = new UserCodeExtractor();
	
		rpcManager.registerRPCProtocolImpl( this, ClientWMProtocol.class );
	}

	//---------------------------------------------------
    // Fields.
    //--------------------------------------------------- 
	
	//private static final Logger LOG = Logger.getLogger( WorkloadManager.class ); 
	
	private final MachineDescriptor machine;
	
	public final IOManager ioManager; 

	public final RPCManager rpcManager;
	
	public final UserCodeExtractor codeExtractor;
	
	//---------------------------------------------------
    // Private.
    //---------------------------------------------------	

	// TODO: check if connections already exist.
	
	// TODO: detect failure if not serializable classes are sent over network 

	@Override
	public void submitProgram( final List<UserCode> userCode,
							   final List<TaskDescriptor> tasks, 
							   final List<TaskBindingDescriptor> bindings ) {

		final List<WM2TMProtocol> tmList = new ArrayList<WM2TMProtocol>();
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
		}
	}
}
