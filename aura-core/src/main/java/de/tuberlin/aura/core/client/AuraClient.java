package de.tuberlin.aura.core.client;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskBindingDescriptor;
import de.tuberlin.aura.core.descriptors.Descriptors.TaskDescriptor;
import de.tuberlin.aura.core.iosystem.IOManager;
import de.tuberlin.aura.core.iosystem.RPCManager;
import de.tuberlin.aura.core.protocols.ClientWMProtocol;
import de.tuberlin.aura.core.task.usercode.UserCode;
import de.tuberlin.aura.core.task.usercode.UserCodeExtractor;

public final class AuraClient {

	//---------------------------------------------------
    // Constructors.
    //---------------------------------------------------
	
	public AuraClient( final MachineDescriptor clientMachine, final MachineDescriptor workloadManager ) {
		// sanity check.
		if( clientMachine == null )
			throw new IllegalArgumentException( "clientMachine == null" );
		if( workloadManager == null )
			throw new IllegalArgumentException( "workloadManager == null" );
		
		this.ioManager = new IOManager( clientMachine );
		
		this.rpcManager = new RPCManager( ioManager );
		
		this.codeExtractor = new UserCodeExtractor();
		
		rpcManager.connectToMessageServer( workloadManager );
		
		clientProtocol = rpcManager.getRPCProtocolProxy( ClientWMProtocol.class, workloadManager );
	
		LOG.info( "Started AURA client" );
	}
	
	//---------------------------------------------------
    // Fields.
    //--------------------------------------------------- 
	
	private static final Logger LOG = Logger.getLogger( AuraClient.class ); 
	
	public final IOManager ioManager; 

	public final RPCManager rpcManager;
	
	public final ClientWMProtocol clientProtocol;
	
	public final UserCodeExtractor codeExtractor;
	
	//---------------------------------------------------
    // Public.
    //---------------------------------------------------
		
	public void submitProgram( final List<Class<?>> userClasses, 
							   final List<TaskDescriptor> tasks, 
							   final List<TaskBindingDescriptor> bindings ) {
		// sanity check.
		if( userClasses == null )
			throw new IllegalArgumentException( "userClasses == null" );
		
		final List<UserCode> userCodeList = new ArrayList<UserCode>();		
		for( final Class<?> clazz : userClasses ) 			
			userCodeList.add( codeExtractor.extractUserCodeClass( clazz ) );
		
		clientProtocol.submitProgram( userCodeList, tasks, bindings );
	}
}
