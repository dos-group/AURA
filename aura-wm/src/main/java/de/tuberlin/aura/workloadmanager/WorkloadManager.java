package de.tuberlin.aura.workloadmanager;

import de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;
import de.tuberlin.aura.core.iosystem.IOManager;

public class WorkloadManager {

	//---------------------------------------------------
    // Constructors.
    //---------------------------------------------------
	
	public WorkloadManager( final MachineDescriptor machine ) {
		// sanity check.
		if( machine == null )
			throw new IllegalArgumentException( "machine must not be null" );
		
		this.machine = machine;
			
		this.ioManager = new IOManager( this.machine );
	}

	//---------------------------------------------------
    // Fields.
    //--------------------------------------------------- 
	
	//private static final Logger LOG = Logger.getLogger( WorkloadManager.class ); 
	
	private final MachineDescriptor machine;
	
	public final IOManager ioManager; 

	//---------------------------------------------------
    // Public.
    //---------------------------------------------------	
	
}
