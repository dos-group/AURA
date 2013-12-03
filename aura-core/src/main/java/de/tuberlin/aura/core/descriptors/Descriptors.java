package de.tuberlin.aura.core.descriptors;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public final class Descriptors {

	// Disallow instantiation.
	private Descriptors() {}
	
	/**
	 *  
	 */
	public static final class MachineDescriptor implements Serializable {
		
		private static final long serialVersionUID = -826435800717651651L;

		public MachineDescriptor( UUID uid, InetAddress address, int dataPort, int controlPort ) {
			// sanity check.
			if( uid == null )
				throw new IllegalArgumentException( "uid must not be null" );
			if( address == null )
				throw new IllegalArgumentException( "address must not be null" );
			
			this.uid = uid;
			
			this.address = address;
			
			this.dataPort = dataPort;
			
			this.controlPort = controlPort;
			
			this.dataAddress = new InetSocketAddress( address, dataPort );
			
			this.controlAddress = new InetSocketAddress( address, controlPort );
		}
		
		public final UUID uid; 
		
		public final int dataPort;
		
		public final int controlPort;
		
		public final InetAddress address;
		
		// TODO: redundant...
		public final InetSocketAddress dataAddress;
		
		public final InetSocketAddress controlAddress;
	
		@Override
		public boolean equals( Object other ) { 
			if( this == other ) return true; 
			if( other == null ) return false; 
			if( other.getClass() != getClass() ) return false;
		
			if( !( uid.equals( ( (MachineDescriptor)other ).uid ) ) ) 
				return false; 
			if( !( dataAddress.equals( ( (MachineDescriptor)other ).dataAddress ) ) ) 
				return false; 
			return true; 
		}
		
		@Override
		public String toString() {
			return (new StringBuilder())
					.append( "MachineDescriptor = {" )
					.append( " uid = " + uid.toString() + ", " )
					.append( " netAddress = " + dataAddress )
					.append( " }" ).toString();
		}		
	}
	
	/**
	 * 
	 */
	public static final class TaskDescriptor implements Serializable {
		
		private static final long serialVersionUID = 7425151926496852885L;

		public TaskDescriptor( MachineDescriptor machine, UUID uid, String name ) {
			// sanity check.
			if( machine == null )
				throw new IllegalArgumentException( "machine must not be null" );
			if( uid == null )
				throw new IllegalArgumentException( "uid must not be null" );
			if( name == null )
				throw new IllegalArgumentException( "name must not be null" );
			
			this.machine = machine;
			
			this.uid = uid;
			
			this.name = name;
		}
		
		public final MachineDescriptor machine;
		
		public final UUID uid;
		
		public final String name;
		
		@Override
		public boolean equals( Object other ) { 
			if( this == other ) return true; 
			if( other == null ) return false; 
			if( other.getClass() != getClass() ) return false;
		
			if( !( machine.equals( ( (TaskDescriptor)other ).machine ) ) ) 
				return false; 
			if( !( uid.equals( ( (TaskDescriptor)other ).uid ) ) ) 
				return false; 
			if( !( name.equals( ( (TaskDescriptor)other ).name ) ) ) 
				return false; 
			return true; 
		}
		
		@Override
		public String toString() {
			return (new StringBuilder())
					.append( "TaskDescriptor = {" )
					.append( " machine = " + machine.toString() + ", " )
					.append( " uid = " + uid.toString() + ", " )
					.append( " name = " + name )
					.append( " }" ).toString();
		}
	}
	
	/**
	 * 
	 */
	public static final class TaskBindingDescriptor implements Serializable {
		
		private static final long serialVersionUID = -2803770527065206844L;

		public TaskBindingDescriptor( TaskDescriptor task, List<TaskDescriptor> inputs, List<TaskDescriptor> outputs ) {
			// sanity check.
			if( task == null )
				throw new IllegalArgumentException( "taskID must not be null" );
			if( inputs == null )
				throw new IllegalArgumentException( "inputs must not be null" );
			if( outputs == null )
				throw new IllegalArgumentException( "outputs must not be null" );
			
			this.task = task;
			
			this.inputs = Collections.unmodifiableList( inputs );
			
			this.outputs = Collections.unmodifiableList( outputs );;
		}		
		
		public final TaskDescriptor task;
		
		public final List<TaskDescriptor> inputs;
		
		public final List<TaskDescriptor> outputs;
		
		@Override
		public boolean equals( Object other ) { 
			if( this == other ) return true; 
			if( other == null ) return false; 
			if( other.getClass() != getClass() ) return false;
		
			if( !( task.equals( ( (TaskBindingDescriptor)other ).task ) ) ) 
				return false; 
			if( !( inputs.equals( ( (TaskBindingDescriptor)other ).inputs ) ) ) 
				return false; 
			if( !( outputs.equals( ( (TaskBindingDescriptor)other ).outputs ) ) ) 
				return false; 
			return true; 
		}
		
		@Override
		public String toString() {
			return (new StringBuilder())
					.append( "TaskBindingDescriptor = {" )
					.append( " task = " + task.toString() + ", " )
					.append( " inputChannels = " + inputs.toString() + ", " )
					.append( " outputChannels = " + outputs.toString() )
					.append( " }" ).toString();
		}
	}
}
