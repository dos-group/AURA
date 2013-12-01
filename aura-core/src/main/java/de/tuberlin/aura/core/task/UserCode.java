package de.tuberlin.aura.core.task;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public final class UserCode implements Serializable {
	
	private static final long serialVersionUID = 4279439116924482785L;

	public UserCode( final String className, final List<String> classDependencies, final byte[] byteCode ) {
		// sanity check.
		if( className == null )
			throw new IllegalArgumentException( "className == null" );
		if( classDependencies == null )
			throw new IllegalArgumentException( "classDependencies == null" );
		if( byteCode == null )
			throw new IllegalArgumentException( "byteCode == null" );
			
		this.className = className;
		
		this.classDependencies = Collections.unmodifiableList( classDependencies );
		
		this.byteCode = byteCode;
	}

	public final String className;

	public final List<String> classDependencies; 
	
	public final byte[] byteCode;	
}
